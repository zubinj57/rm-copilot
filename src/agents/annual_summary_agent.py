from __future__ import annotations

import json
import math
import re
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from langchain.schema import HumanMessage

from src.common import (
    llm,
    SYSTEM_PREFIX,
    JSON_SCHEMA_INSTRUCTION,
    getChromaByPropertyCode,
)

SYSTEM = SYSTEM_PREFIX + " Scope: annual summary KPIs, trends, YoY/STLY/OTB analyses. Return JSON."

_MONTHS = {
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
"dec": 12, "december": 12,
}

_MONTH_RE = re.compile(r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b", re.I)
_YEAR_RE = re.compile(r"\b(20\d{2}|19\d{2})\b")


@dataclass
class QuerySpec:
    month: Optional[int]
    year: Optional[int]
    text: str

def _parse_query(user_question: str, as_of: str) -> QuerySpec:
    ql = (user_question or "").lower()
    # month/year from query
    m = _MONTH_RE.search(ql)
    month = _MONTHS.get(m.group(1)[:3], None) if m else None
    y = _YEAR_RE.search(ql)
    year = int(y.group(1)) if y else None

    # If still missing, infer from AsOfDate
    try:
        asof_dt = datetime.strptime(as_of, "%Y-%m-%d")
        year = year or asof_dt.year
    except Exception:
        pass

    return QuerySpec(month=month, year=year, text=user_question)

def _retrieve_docs(property_code: str, spec: QuerySpec, widen: int = 0) -> Tuple[List[str], List[str]]:
    """Return (docs, source_ids). Widen=0 exact; 1 nearby months; 2 whole year; 3 neighboring years."""
    chroma = getChromaByPropertyCode(property_code)
    query_terms = [spec.text]

    filters: Dict[str, Any] = {}
    if spec.year:
        filters["year"] = spec.year
    if spec.month:
        filters["month"] = spec.month

    def _do_search(ft: Dict[str, Any]) -> Tuple[List[str], List[str]]:
        where = {"$and": [{"year": {"$eq": 2025}}, {"month": {"$eq": 1}}]}
        res = chroma.similarity_search_with_relevance_scores(
            query_terms[0], k=12, filter={"where": where}
        )
        docs = [d.page_content for d, _ in res]
        srcs = [getattr(d, "metadata", {}).get("source", "") for d, _ in res]
        return docs, srcs

    # Try in widening rings
    if widen == 0:
        return _do_search(filters)
    elif widen == 1:
    # nearby months, same year
        docs, src = _do_search(filters)
        if docs:
            return docs, src
        if spec.year and spec.month:
            for delta in (-1, 1, -2, 2):
                mm = ((spec.month - 1 + delta) % 12) + 1
                ft = {"year": spec.year, "month": mm}
                docs, src = _do_search(ft)
                if docs:
                    return docs, src
        return [], []
    elif widen == 2:
        ft = {"year": spec.year} if spec.year else {}
        return _do_search(ft)
    else:
        # neighboring years
        if spec.year:
            for y in (spec.year - 1, spec.year + 1):
                ft = {"year": y}
                docs, src = _do_search(ft)
                if docs:
                    return docs, src
        return [], []
    
def _confidence_from(docs: List[str], answer_text: str, kpi_count: int) -> float:
    base = 0.35 if docs else 0.15
    base += 0.05 * min(kpi_count, 6)
    # small nudge for longer answers (bounded)
    base += min(len(answer_text) / 1200.0, 0.15)
    return float(max(0.0, min(0.98, base)))

def agent_handle(user_question: str, propertyCode: str, AsOfDate: str, force_broaden: bool = False, **_) -> Dict[str, Any]:
    spec = _parse_query(user_question, AsOfDate)

    # Retrieval with widening rings
    docs: List[str] = []
    srcs: List[str] = []
    for widen in ([0, 1, 2, 3] if force_broaden else [0, 1, 2]):
        docs, srcs = _retrieve_docs(propertyCode, spec, widen=widen)
        if docs:
            break

    SYSTEM_MSG = SYSTEM + f"\nIf month/year are unclear, infer from AsOfDate={AsOfDate}. Always return valid JSON."

    schema_hint = (
        JSON_SCHEMA_INSTRUCTION
        + "\nRequired JSON keys: answer_text (string), kpis (array of {name,value,unit,period}), explanations (array of strings), confidence (0..1), sources (array), suggested_actions (array)."
        + "\nIf evidence is thin, explain what’s missing and propose specific actions (e.g., re-ingest month X 20YY)."
    )

    content = "\n\n".join([
        f"Question: {user_question}",
        f"Property: {propertyCode}",
        f"AsOfDate: {AsOfDate}",
        ("Evidence:\n" + "\n---\n".join(docs[:8])) if docs else "Evidence: (no direct monthly docs found in narrow filters)",
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

    # Normalize/guarantee structure
    answer_text = str(parsed.get("answer_text", "")).strip()
    kpis = parsed.get("kpis") or []
    explanations = parsed.get("explanations") or []
    suggested = parsed.get("suggested_actions") or []

    if not docs:
        explanations = explanations + [
            "No monthly/yearly documents were retrieved under the initial filters.",
        ]
        if spec.year and spec.month:
            suggested = suggested + [
                f"Check ingestion for {spec.year}-{spec.month:02d}.",
                "Try removing the month filter to use full-year evidence.",
            ]
        else:
            suggested = suggested + [
                "Specify a month and year (e.g., ‘January 2025’) or request the full-year summary.",
            ]

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


