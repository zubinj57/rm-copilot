from __future__ import annotations

import concurrent.futures
import re
from typing import Any, Dict, List, Tuple

from utils.logger import get_custom_logger

# Import your agents as modules (ensure these import paths are valid in your project)
from .agents import (
daily_summary_agent,
performance_monitor_agent,
annual_summary_agent,
)

logger = get_custom_logger(name="Orchestrator")


_MONTH_RE = re.compile(r"\b(jan(?:uary)?|feb(?:ruary)?|mar(?:ch)?|apr(?:il)?|may|jun(?:e)?|jul(?:y)?|aug(?:ust)?|sep(?:t(?:ember)?)?|oct(?:ober)?|nov(?:ember)?|dec(?:ember)?)\b", re.I)
_YEAR_RE = re.compile(r"\b(20\d{2}|19\d{2})\b")

TRIGGERS = {
"daily": {"revpar", "adr", "occupancy", "daily", "today", "OTB", "pickup", "pick-up", "pick up", "forecast"},
"performance": {"performance", "property score", "score", "kpi"},
"annual": {"annual", "monthly", "month", "yearly", "year", "forecast", "trend", "OTB", "stly"},
}

def _extract_entities(q: str) -> Dict[str, Any]:
    ql = q.lower()
    month = None
    year = None
    y = None
    m = _MONTH_RE.search(ql)
    if m:
        month = m.group(1)
        y = _YEAR_RE.search(ql)
    if y:
        year = int(y.group(1))
    return {"month": month, "year": year}

def _route_agents(query: str) -> List[Any]:
    ql = query.lower()
    selected = []
    # Daily when explicitly daily KPIs appear
    if any(t in ql for t in TRIGGERS["daily"]):
        selected.append(daily_summary_agent)
    # Performance monitor for performance/score language
    if any(t in ql for t in TRIGGERS["performance"] | TRIGGERS["daily"]):
        selected.append(performance_monitor_agent)
    # Annual for any temporal/YoY/OTB language OR when question mentions a month/year explicitly
    ents = _extract_entities(query)
    mentions_time = any(t in ql for t in TRIGGERS["annual"]) or ents["month"] or ents["year"]
    if mentions_time:
        selected.append(annual_summary_agent)

    # Default ensemble if nothing triggered
    if not selected:
        selected = [daily_summary_agent, performance_monitor_agent, annual_summary_agent]
    # Deduplicate while preserving order
    seen = set()
    out = []
    for m in selected:
        if m not in seen:
            out.append(m)
            seen.add(m)
    return out

def _score_result(r: Dict[str, Any]) -> float:
    if not isinstance(r, dict):
        return 0.0
    conf = float(r.get("confidence", 0.0) or 0.0)
    kpis = r.get("kpis") or []
    sources = r.get("sources") or []
    # reward evidence richness
    bonus = 0.05 * min(len(kpis), 10) + 0.04 * min(len(sources), 10)
    return max(0.0, min(1.0, conf + bonus))

def _aggregate(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not results:
        return {
            "answer_text": "No agents returned a result.",
            "kpis": [],
            "explanations": [],
            "confidence": 0.0,
            "sources": [],
            "suggested_actions": [
            "Try widening the date or property filters.",
            "Re-run ingestion to ensure documents exist for the requested period.",
            ],
            "requires_review": True,
        }

    # Merge text
    parts: List[str] = []
    agg: Dict[str, Any] = {
        "kpis": [],
        "explanations": [],
        "sources": [],
        "suggested_actions": [],
    }

    confidences: List[float] = []
    for r in results:
        if not isinstance(r, dict):
            logger.warning("Agent returned non-dict result: %r", r)
            continue
        if txt := r.get("answer_text"):
            parts.append(str(txt).strip())
        agg["kpis"].extend(r.get("kpis", []) or [])
        agg["explanations"].extend(r.get("explanations", []) or [])
        agg["sources"].extend(r.get("sources", []) or [])
        agg["suggested_actions"].extend(r.get("suggested_actions", []) or [])
        confidences.append(_score_result(r))

    # Deduplicate sources while preserving order
    seen = set()
    dedup_sources = []
    for s in agg["sources"]:
        if s not in seen:
            dedup_sources.append(s)
            seen.add(s)

    agg["answer_text"] = "\n\n".join([p for p in parts if p]) or "No direct answer available from agents."
    agg["confidence"] = round(sum(confidences) / len(confidences), 3) if confidences else 0.0
    agg["sources"] = dedup_sources

    # Soft flag for human review
    agg["requires_review"] = agg["confidence"] < 0.68

    # If still weak, add actionable next steps
    if agg["requires_review"]:
        agg.setdefault("suggested_actions", [])
        agg["suggested_actions"] += [
        "Check if annual summaries were ingested for the requested month/year.",
        "Try removing the month filter; ask for the year summary only.",
        "If you meant a forecast, include a clear time window (e.g., ‘Q4 2025’).",
        ]

    return agg

def handle_query(user_query: str, propertyCode: str, AsOfDate: str, **kwargs) -> Dict[str, Any]:
    """Entry point used by FastAPI.
    Runs selected agents in parallel, aggregates results, and returns a structured dict.
    """
    q = (user_query or "").strip()
    logger.info("handle_query: %s | property=%s | AsOfDate=%s", q, propertyCode, AsOfDate)

    agents = _route_agents(q)

    results: List[Dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(agents) or 1)) as ex:
        futs = []
        for m in agents:
            try:
                futs.append(ex.submit(m.agent_handle, q, propertyCode, AsOfDate, **kwargs))
            except Exception as e:
                logger.exception("Failed to schedule agent %s: %s", getattr(m, "__name__", m), e)
        for f in concurrent.futures.as_completed(futs, timeout=25):
            try:
                r = f.result(timeout=1)
                results.append(r)
            except Exception as e:
                logger.warning("Agent future failed: %s", e)

    agg = _aggregate(results)

    # As a last resort, if answer_text looks like a generic ‘insufficient data’ from everyone,
    # try a single annual agent fallback with broadened retrieval (the agent handles widening).
    if not results or (agg["confidence"] < 0.4 and "insufficient data" in agg["answer_text"].lower()):
        try:
            logger.info("Triggering last-resort annual_summary_agent fallback with broadened search")
            fallback = annual_summary_agent.agent_handle(q + " (broaden search)", propertyCode, AsOfDate, force_broaden=True)
            agg = _aggregate([fallback])
        except Exception:
            logger.exception("Fallback annual agent also failed")
    return agg

