# annual_summary_agent.py
from __future__ import annotations
import os
import re
import logging
from typing import Dict, Any, List, Tuple, Optional

from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_community.vectorstores import Chroma
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains import create_retrieval_chain

from ..common import getChromaByPropertyCode
from utils.logger import get_custom_logger
logger = get_custom_logger(name="annual_summary_agent")

# -----------------------
# Config
# -----------------------
DEFAULT_PERSIST_ROOT = os.environ.get("CHROMA_DIR", "./chroma")
MODEL_NAME = os.environ.get("LC_CHAT_MODEL", "gpt-4o-mini")
EMBED_MODEL = os.environ.get("LC_EMBED_MODEL", "text-embedding-3-small")

SYSTEM = """You are a hotel analytics assistant.
Use ONLY the provided context to answer. If the answer isn’t in the context, say you don’t know.
Cite sources as [S1], [S2], ... using the order the documents are provided in the context block."""

# Month normalization
MONTH_MAP = {
    "jan": 1, "january": 1,
    "feb": 2, "february": 2,
    "mar": 3, "march": 3,
    "apr": 4, "april": 4,
    "may": 5,
    "jun": 6, "june": 6,
    "jul": 7, "july": 7,
    "aug": 8, "august": 8,
    "sep": 9, "sept": 9, "september": 9,
    "oct": 10, "october": 10,
    "nov": 11, "november": 11,
    "dec": 12, "december": 12
}

# -----------------------
# Utilities
# -----------------------


def _parse_month_year(text: str) -> Tuple[Optional[int], Optional[int]]:
    t = text.lower().strip()

    # year
    year = None
    m = re.search(r"(20\d{2})", t)
    if m:
        year = int(m.group(1))

    # month number or name
    month = None
    mnum = re.search(r"\b(1[0-2]|0?[1-9])\b", t)
    # prefer named month if present to avoid catching dates in other places
    mname = None
    for name, num in MONTH_MAP.items():
        if re.search(rf"\b{name}\b", t):
            mname = num
            break

    if mname:
        month = mname
    elif mnum:
        # if a numeric month is present BUT the text also contains a day (like 2025-09-25)
        # we keep it anyway—metadata filter will still work at month granularity
        month = int(mnum.group(1))

    return month, year

def _widen_filters(base: Dict[str, Any], widen: int) -> Dict[str, Any]:
    """
    widen=0: exact (year + month if present)
    widen=1: prev/this/next month (same year)
    widen=2: whole year
    widen=3: neighbor years (+/-1)
    """
    out = dict(base)
    yr = base.get("year")
    mo = base.get("month")
    if widen == 0:
        return out
    if widen == 1 and yr and mo:
        out.pop("month", None)
        out["month_any_of"] = [((mo - 2) % 12) + 1, mo, (mo % 12) + 1]  # prev, this, next
    elif widen == 2 and yr:
        out.pop("month", None)
        out.pop("month_any_of", None)
    elif widen == 3 and yr:
        out.pop("month", None)
        out.pop("month_any_of", None)
        out["year_any_of"] = [yr - 1, yr, yr + 1]
    return out

def _to_chroma_where(meta: Dict[str, Any]) -> Dict[str, Any]:
    where = {}
    if "year" in meta: where["year"] = meta["year"]
    if "month" in meta: where["month"] = meta["month"]
    if "month_any_of" in meta: where["month"] = {"$in": meta["month_any_of"]}
    if "year_any_of" in meta: where["year"] = {"$in": meta["year_any_of"]}
    return where

def _build_rag_chain(retriever) -> Any:
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM + "\n\n<CONTEXT>\n{context}\n</CONTEXT>"),
        ("human", "{input}")
    ])
    llm = ChatOpenAI(model=MODEL_NAME, temperature=0)
    doc_chain = create_stuff_documents_chain(llm, prompt)
    return create_retrieval_chain(retriever, doc_chain)

def _make_mmr_retriever(vs: Chroma, where: Optional[Dict[str, Any]] = None, *, k=8, fetch_k=48, lambda_mult=0.7):
    return vs.as_retriever(
        search_type="mmr",
        search_kwargs={
            "k": k,
            "fetch_k": fetch_k,
            "lambda_mult": lambda_mult,
            "where": where or {}
        }
    )

# -----------------------
# Public Agent API
# -----------------------
def agent_handle(
    user_question: str,
    propertyCode: str,
    AsOfDate: Optional[str] = None,
    force_broaden: bool = False
) -> Dict[str, Any]:
    """
    Returns:
      {
        "answer_text": str,
        "kpis": list,
        "explanations": list,
        "confidence": float,
        "sources": list[str],
        "suggested_actions": list[str],
        "requires_review": bool
      }
    """
    try:
        # If user didn't give a year, fall back to AsOfDate's year
        default_year: Optional[int] = None
        if AsOfDate:
            try:
                default_year = int(AsOfDate[:4])
            except Exception:
                default_year = None

        # ✅ Open the exact same store/collection as ingestion
        #    (persist dir resolves to repo_root/<propertyCode> inside the helper)
        vs = getChromaByPropertyCode(
            propertyCode=propertyCode,
            collection_name="annual_summary",          # <-- must match ingestion
            property_store_dir=f"./{propertyCode}",    # <-- repo_root/<propertyCode>
        )

        # Parse query for month/year (prefer named months)
        month, year = _parse_month_year(user_question)
        if year is None:
            year = default_year

        base: Dict[str, Any] = {}
        if year is not None:
            base["year"] = int(year)
        if month is not None:
            base["month"] = int(month)

        # Widening passes
        passes = [0, 1, 2, 3] if not force_broaden else [2, 3, 1, 0]

        picked_docs: List[Document] = []
        used_where: Dict[str, Any] = {}
        used_widen: Optional[int] = None

        for widen in passes:
            filt = _widen_filters(base, widen)
            where = _to_chroma_where(filt)
            retriever = _make_mmr_retriever(
                vs, where=where, k=8, fetch_k=64, lambda_mult=0.65
            )

            # ✅ modern API (no deprecation warning)
            docs = retriever.invoke(user_question)
            logger.info(f"[{propertyCode}] widen={widen} where={where} -> {len(docs)} docs")

            if len(docs) >= 4 or (widen >= 2 and len(docs) >= 2):
                picked_docs = docs
                used_where = where
                used_widen = widen

                chain = _build_rag_chain(retriever)
                resp = chain.invoke({"input": user_question})
                answer = (resp.get("answer") or resp.get("result") or "").strip()
                ctx_docs: List[Document] = resp.get("context") or picked_docs

                sources: List[str] = []
                for d in ctx_docs:
                    src = d.metadata.get("source") or d.metadata.get("path") or d.metadata.get("id") or "unknown"
                    if src not in sources:
                        sources.append(src)

                if answer and answer.lower() not in {"i don't know", "i do not know", "unknown"}:
                    confidence = 0.65 if widen <= 1 else 0.52
                    return {
                        "answer_text": answer,
                        "kpis": [],
                        "explanations": [],
                        "confidence": round(confidence, 3),
                        "sources": sources[:12],
                        "suggested_actions": [],
                        "requires_review": False,
                    }
                # else: keep widening

        logger.warning(f"[{propertyCode}] Insufficient context after widening. Last where={used_where}")
        return {
            "answer_text": "insufficient data",
            "kpis": [],
            "explanations": [],
            "confidence": 0.0,
            "sources": [],
            "suggested_actions": [
                "Re-run ingestion for the requested month/year or property.",
                "Broaden the query (e.g., remove exact month) or try a nearby month.",
                "Verify documents have metadata: year, month, propertyCode, source."
            ],
            "requires_review": True,
        }

    except Exception as e:
        logger.exception("annual_summary_agent failed")
        return {
            "answer_text": f"Error: {e}",
            "kpis": [],
            "explanations": [],
            "confidence": 0.0,
            "sources": [],
            "suggested_actions": ["Check server logs and environment variables."],
            "requires_review": True,
        }
