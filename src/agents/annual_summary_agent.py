from __future__ import annotations
import os, re, json, calendar
from typing import Dict, Any, List, Tuple, Optional
from langchain_openai import ChatOpenAI
from langchain_chroma import Chroma
from langchain_core.prompts import ChatPromptTemplate
from langchain_core.documents import Document
from langchain.chains.combine_documents import create_stuff_documents_chain
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
- Explicitly quote the month and year where the true maximum/minimum occurs.
- Never infer from words like "peak" or "strong"; rely strictly on numbers.

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
    if m:
        year = int(m.group(1))
    month = None
    for name, num in MONTH_MAP.items():
        if name and name in text:
            month = num
            break
    return month, year


def _get_all_year_docs(vs: Chroma, year: int) -> list[Document]:
    """Retrieve all documents for a given year and convert them into LangChain Document objects."""
    res = vs._collection.get(where={"year": {"$eq": year}}, include=["documents", "metadatas"])
    if not res or not res.get("documents"):
        return []

    docs: list[Document] = []
    for raw_doc, meta in zip(res["documents"], res["metadatas"]):
        content = raw_doc if isinstance(raw_doc, str) else json.dumps(raw_doc, ensure_ascii=False)
        meta = meta or {}
        meta.setdefault("year", year)
        docs.append(Document(page_content=content, metadata=meta))

    # Sort for quick numeric comparisons
    docs.sort(key=lambda d: float(d.metadata.get("current_adr", 0) or 0), reverse=True)
    logger.info(f"[DEBUG] Retrieved {len(docs)} docs for {year}: {[d.metadata.get('month') for d in docs]}")
    return docs


# ---------------- MAIN AGENT ----------------
def agent_handle(user_question: str, propertyCode: str,
                 AsOfDate: Optional[str] = None,
                 force_broaden: bool = False) -> Dict[str, Any]:
    try:
        default_year = int(AsOfDate[:4]) if AsOfDate else None
        month, year = _parse_month_year(user_question)
        year = year or default_year

        vs = getChromaByPropertyCode(propertyCode, "annual_summary", f"./{propertyCode}")

        if not year:
            return {
                "answer_text": "Year not specified; please mention which year to analyze.",
                "kpis": [], "explanations": [], "confidence": 0.0,
                "sources": [], "suggested_actions": ["Rephrase question with year."],
                "requires_review": True,
            }

        all_docs = _get_all_year_docs(vs, year)
        if not all_docs:
            return {
                "answer_text": f"No annual summary data found for {year}.",
                "kpis": [], "explanations": [], "confidence": 0.0,
                "sources": [], "suggested_actions": ["Re-ingest data for this year."],
                "requires_review": True,
            }

        # Compute numeric max (ADR)
        max_doc = max(all_docs, key=lambda d: float(d.metadata.get("current_adr", 0) or 0))
        top_month = max_doc.metadata.get("month")
        top_adr = float(max_doc.metadata.get("current_adr", 0) or 0)
        logger.info(f"[{propertyCode}] Highest ADR in {year}: {top_month} = {top_adr}")

        # Build the LLM + document-stuffing chain
        llm = ChatOpenAI(model=MODEL_NAME, temperature=0)

        prompt = ChatPromptTemplate.from_messages([
            ("system", SYSTEM + "\n\n<CONTEXT>\n{context}\n</CONTEXT>"),
            ("human", "{input}")
        ])

        chain = create_stuff_documents_chain(llm, prompt)


        # ✅ Pass the Document objects directly (fixes AttributeError)
        resp = chain.invoke({
            "context": all_docs,
            "input": user_question
        })
        logger.info("🧠 LLM Raw Response: %s", resp)

        # Extract answer text
        answer = ""
        if isinstance(resp, dict):
            answer = resp.get("output_text") or resp.get("answer") or resp.get("result", "")
        elif isinstance(resp, str):
            answer = resp
        answer = answer.strip()

        # Try to parse JSON response
        try:
            parsed = json.loads(answer)
        except Exception:
            parsed = None

        def ensure_list(x): return x if isinstance(x, list) else []

        if isinstance(parsed, dict):
            return {
                "answer_text": parsed.get("answer_text", ""),
                "kpis": ensure_list(parsed.get("kpis")),
                "explanations": ensure_list(parsed.get("explanations")),
                "confidence": parsed.get("confidence", 0.9),
                "sources": parsed.get("sources", [d.metadata.get("month") for d in all_docs]),
                "suggested_actions": ensure_list(parsed.get("suggested_actions")),
                "requires_review": parsed.get("requires_review", False),
            }

        # Fallback: model returned plain text
        return {
            "answer_text": answer,
            "kpis": [{"name": "ADR", "value": top_adr, "unit": "USD", "relevance": f"Highest ADR in {year}"}],
            "explanations": [{
                "factor": "Numeric summary pre-computed",
                "impact_percent": None,
                "evidence": f"{top_month} {year} has ADR {top_adr}"
            }],
            "confidence": 0.95,
            "sources": [top_month],
            "suggested_actions": [
                "Use this insight to benchmark rate strategy.",
                "Validate seasonal ADR fluctuations vs compset."
            ],
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
