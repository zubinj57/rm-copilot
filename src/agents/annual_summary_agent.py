from __future__ import annotations
import os, re, json, logging, calendar
from typing import Dict, Any, List, Tuple, Optional
from langchain_openai import ChatOpenAI
from langchain_chroma import Chroma
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains import create_retrieval_chain
from ..common import getChromaByPropertyCode
from utils.logger import get_custom_logger

logger = get_custom_logger("annual_summary_agent")

# ---------------- CONFIG ----------------
MODEL_NAME = os.getenv("LC_CHAT_MODEL", "gpt-4o-mini")

SYSTEM = """
You are RM Copilot, a professional virtual revenue manager.
You analyze retrieved Annual Summary data that includes numeric KPIs such as ADR, Occupancy, and Revenue.

When asked for "highest" or "lowest" metrics:
- Always compare all numeric values in the context before answering.
- Identify the document with the **maximum or minimum numeric value** as relevant.
- Never infer from wording like "peak" or "strong"; rely strictly on numbers.
- Explicitly quote the month and year where the true maximum/minimum occurs.
- If ADR values are listed, compute which month has the largest ADR value.
- Never assume chronological order or semantic priority.

Respond only with valid JSON:
{{
  "answer_text": "...",
  "kpis": [{{"name": "...", "value": ..., "unit": "...", "relevance": "..."}}],
  "explanations": [{{"factor": "...", "impact_percent": ..., "evidence": "..."}}],
  "confidence": float,
  "sources": ["...", "..."],
  "suggested_actions": ["...", "..."],
  "requires_review": bool
}}
"""

MONTH_MAP = {m.lower(): i for i, m in enumerate(calendar.month_name) if m}

# ---------------- HELPERS ----------------
def _parse_month_year(text: str) -> Tuple[Optional[int], Optional[int]]:
    text = text.lower()
    year = None
    m = re.search(r"(20\d{2})", text)
    if m: year = int(m.group(1))
    month = None
    for name, num in MONTH_MAP.items():
        if name and name in text:
            month = num
            break
    return month, year

def _to_chroma_where(meta: Dict[str, Any]) -> Dict[str, Any]:
    where = {}
    if "year" in meta: where["year"] = meta["year"]
    if "month" in meta: where["month"] = meta["month"]
    return where

def _build_chain(retriever) -> Any:
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM + "\n\n<CONTEXT>\n{context}\n</CONTEXT>"),
        ("human", "{input}")
    ])
    llm = ChatOpenAI(model=MODEL_NAME, temperature=0)
    doc_chain = create_stuff_documents_chain(llm, prompt)
    return create_retrieval_chain(retriever, doc_chain)

def _make_mmr_retriever(vs: Chroma, where: Dict[str, Any], k: int = 8):
    return vs.as_retriever(
        search_type="mmr",
        search_kwargs={"k": k, "fetch_k": 48, "lambda_mult": 1.0, "where": where or {}}
    )

def _get_all_year_docs(vs, year: int) -> list[Document]:
    """Fetch all docs for a given year."""
    res = vs._collection.get(
        where={"year": {"$eq": year}},
        include=["documents", "metadatas"]
    )
    if not res or not res.get("documents"):
        return []
    return [Document(page_content=d, metadata=m)
            for d, m in zip(res["documents"], res["metadatas"])]

def _numeric_rank_docs(vs: Chroma, year: int, metric: str = "current_adr") -> List[Document]:
    """Return all docs for that year sorted by metric descending."""
    res = vs._collection.get(where={"year": {"$eq": year}}, include=["documents", "metadatas"])
    if not res or not res.get("documents"):
        return []
    pairs = list(zip(res["metadatas"], res["documents"]))
    pairs.sort(key=lambda x: float(x[0].get(metric, 0) or 0), reverse=True)
    return [Document(page_content=d, metadata=m) for m, d in pairs]

# ---------------- MAIN AGENT ----------------
def agent_handle(user_question: str, propertyCode: str, AsOfDate: Optional[str] = None,
                 force_broaden: bool = False) -> Dict[str, Any]:
    try:
        default_year = int(AsOfDate[:4]) if AsOfDate else None
        month, year = _parse_month_year(user_question)
        year = year or default_year

        vs = getChromaByPropertyCode(propertyCode, "annual_summary", f"./{propertyCode}")
        picked_docs: List[Document] = []

        # Numeric-aware path for "highest"/"lowest" style questions
        if year and "highest" in user_question.lower() and "adr" in user_question.lower():
            all_docs = _get_all_year_docs(vs, year)
            # sort by current_adr descending
            all_docs.sort(key=lambda d: float(d.metadata.get("current_adr", 0) or 0), reverse=True)
            picked_docs = all_docs[:12]  # all months if available
            logger.info(f"[{propertyCode}] Numeric ADR retrieval: top doc = {picked_docs[0].metadata.get('month')} ADR={picked_docs[0].metadata.get('current_adr')}")
        else:
            where = {"year": year} if year else {}
            retriever = _make_mmr_retriever(vs, where)
            picked_docs = retriever.invoke(user_question)

        if not picked_docs:
            return {
                "answer_text": "insufficient data",
                "kpis": [], "explanations": [], "confidence": 0.0,
                "sources": [], "suggested_actions": [
                    "Re-ingest data or verify metadata year/month."
                ],
                "requires_review": True,
            }

        # ✅ Build retriever without duplicate 'where'
        retriever = _make_mmr_retriever(vs, where={"year": year}) if year else vs.as_retriever()

        # ✅ Build the RAG chain once — do NOT add 'where' again here
        chain = _build_chain(retriever)

        # ✅ Run the chain (no duplicate 'where' now)
        resp = chain.invoke({"input": f"{user_question}\n\nFocus only on ADR numeric values and identify the month with the maximum ADR."})
        print("🧠 LLM Raw Response:", resp)
        answer = (resp.get("answer") or resp.get("result") or "").strip()
        print("🧠 LLM Answer:", answer)
        ctx_docs: List[Document] = resp.get("context") or picked_docs

        # ✅ Collect sources for traceability
        sources = []
        for d in ctx_docs:
            src = d.metadata.get("month") or d.metadata.get("id") or "unknown"
            if src not in sources:
                sources.append(src)

        # ✅ Try parsing JSON from the LLM output
        parsed = None
        try:
            parsed = json.loads(answer)
        except Exception:
            pass

        def ensure_list(x): return x if isinstance(x, list) else []

        if isinstance(parsed, dict):
            return {
                "answer_text": parsed.get("answer_text", ""),
                "kpis": ensure_list(parsed.get("kpis")),
                "explanations": ensure_list(parsed.get("explanations")),
                "confidence": parsed.get("confidence", 0.75),
                "sources": parsed.get("sources", sources[:12]),
                "suggested_actions": ensure_list(parsed.get("suggested_actions")),
                "requires_review": parsed.get("requires_review", False),
            }

        # ✅ Fallback plain text
        return {
            "answer_text": answer,
            "kpis": [{"name": "ADR", "value": None, "unit": "USD", "relevance": "summary"}],
            "explanations": [],
            "confidence": 0.6,
            "sources": sources[:12],
            "suggested_actions": [],
            "requires_review": False,
        }

    except Exception as e:
        logger.exception("annual_summary_agent failed")
        return {
            "answer_text": f"Error: {e}",
            "kpis": [], "explanations": [],
            "confidence": 0.0, "sources": [],
            "suggested_actions": ["Check logs and ensure Chroma store exists."],
            "requires_review": True,
        }
