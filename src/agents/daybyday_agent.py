# src/agents/daybyday_agent.py

from __future__ import annotations
import json
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from langchain.schema import HumanMessage
from src.common import llm, SYSTEM_PREFIX, JSON_SCHEMA_INSTRUCTION, getChromaByPropertyCode

SYSTEM = SYSTEM_PREFIX + " Scope: day-by-day KPIs (occupancy, ADR, RevPAR, rooms sold, revenue). Return JSON."

# --- Regex for parsing date queries ---
_DATE_RE = re.compile(r"\b(20\d{2}-\d{2}-\d{2})\b")  # YYYY-MM-DD

@dataclass
class QuerySpec:
    date: Optional[str]
    text: str

def _parse_query(user_question: str, as_of: str) -> QuerySpec:
    ql = (user_question or "").lower()
    m = _DATE_RE.search(ql)
    date_str = None
    if m:
        date_str = m.group(1)
    return QuerySpec(date=date_str, text=user_question)

def _retrieve_docs(property_code: str, as_of_date: str, spec: QuerySpec, widen: bool = False) -> Tuple[List[str], List[str]]:
    chroma = getChromaByPropertyCode(property_code, collection_name="daily_summaries")
    query_terms = [spec.text]

    # Build filter
    filters: Dict[str, Any] = {
        "$and": [
            {"type": {"$eq": "daily_summary"}},
            {"property_code": {"$eq": property_code}},
            {"as_of_date": {"$eq": as_of_date}},
        ]
    }
    if spec.date:
        filters["$and"].append({"date": {"$eq": spec.date}})

    def _do_search(ft: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        res = chroma.similarity_search_with_relevance_scores(query_terms[0], k=12, filter=ft or None)
        docs = [d.page_content for d, _ in res]
        srcs = [getattr(d, "metadata", {}).get("source", "") for d, _ in res]
        return docs, srcs

    docs, srcs = _do_search(filters)

    # Widen search (remove date filter) if nothing found
    if not docs and widen:
        ft = {"$and": [
            {"type": {"$eq": "daily_summary"}},
            {"property_code": {"$eq": property_code}},
            {"as_of_date": {"$eq": as_of_date}},
        ]}
        docs, srcs = _do_search(ft)

    return docs, srcs

def _confidence_from(docs: List[str], answer_text: str, kpi_count: int) -> float:
    base = 0.35 if docs else 0.15
    base += 0.05 * min(kpi_count, 6)
    base += min(len(answer_text) / 1200.0, 0.15)
    return float(max(0.0, min(0.98, base)))

def agent_handle(user_question: str, propertyCode: str, AsOfDate: str, force_broaden: bool = False, **_) -> Dict[str, Any]:
    spec = _parse_query(user_question, AsOfDate)

    docs: List[str] = []
    srcs: List[str] = []
    docs, srcs = _retrieve_docs(propertyCode, AsOfDate, spec, widen=force_broaden)

    SYSTEM_MSG = SYSTEM + f"\nIf date is unclear, infer from AsOfDate={AsOfDate}. Always return valid JSON."

    schema_hint = (
        JSON_SCHEMA_INSTRUCTION
        + "\nRequired JSON keys: answer_text (string), kpis (array of {name,value,unit,date}), "
        + "explanations (array of strings), confidence (0..1), sources (array), suggested_actions (array)."
    )

    content = "\n\n".join([
        f"Question: {user_question}",
        f"Property: {propertyCode}",
        f"AsOfDate: {AsOfDate}",
        ("Evidence:\n" + "\n---\n".join(docs[:8])) if docs else "Evidence: (no daily docs found)"
    ])

    messages = [
        HumanMessage(content=SYSTEM_MSG + "\n\n" + schema_hint + "\n\n" + content),
    ]

    try:
        resp = llm.generate([[m for m in messages]])
        text = resp.generations[0][0].message.content
        try:
            parsed = json.loads(text)
        except Exception:
            parsed = {"answer_text": text}
    except Exception as e:
        parsed = {"answer_text": f"LLM error: {e}"}

    answer_text = str(parsed.get("answer_text", "")).strip()
    kpis = parsed.get("kpis") or []
    explanations = parsed.get("explanations") or []
    suggested = parsed.get("suggested_actions") or []

    if not docs:
        explanations.append("No daily documents were retrieved under the filters.")
        suggested.append("Check ingestion for the specific date or remove date filter for broader view.")

    confidence = parsed.get("confidence")
    if not isinstance(confidence, (int, float)):
        confidence = _confidence_from(docs, answer_text, kpi_count=len(kpis))

    result = {
        "answer_text": answer_text or "No direct answer produced.",
        "kpis": kpis,
        "explanations": explanations,
        "confidence": float(round(confidence, 3)),
        "sources": srcs[:12],
        "suggested_actions": suggested,
    }
    return result
