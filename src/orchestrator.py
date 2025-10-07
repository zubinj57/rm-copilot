# orchestrator.py
from __future__ import annotations
import logging
from typing import Dict, Any, List

from .agents.annual_summary_agent import agent_handle as annual_agent
from utils.logger import get_custom_logger

logger = get_custom_logger(name="orchestrator")

def _aggregate(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Simple aggregator: pick the best non-insufficient answer with the highest confidence."""
    if not results:
        return _insufficient()

    # Filter valid answers
    valid = [
        r for r in results
        if isinstance(r, dict)
        and r.get("answer_text")
        and "insufficient data" not in r.get("answer_text", "").lower()
        and not r.get("requires_review", False)
    ]
    if not valid:
        # Pick the 'least bad' (highest confidence) to retain suggested_actions, etc.
        best = max(results, key=lambda r: r.get("confidence", 0.0))
        return best

    best = max(valid, key=lambda r: r.get("confidence", 0.0))
    return best

def _insufficient() -> Dict[str, Any]:
    return {
        "answer_text": "insufficient data",
        "kpis": [],
        "explanations": [],
        "confidence": 0.0,
        "sources": [],
        "suggested_actions": [],
        "requires_review": True,
    }

def simple_route(query: str):
    q = query.lower()
    agents = []
    # annual-style queries get routed to annual agent
    if any(x in q for x in ["annual", "monthly", "month", "year", "yearly", "stly", "forecast", "trend", "kpi", "performance", "otb", "rooms sold", "room sold", "adr", "revpar", "occupancy"]):
        agents.append("annual_summary")
    if not agents:
        agents = ["annual_summary"]  # default
    return agents

def handle_query(query: str, propertyCode: str, AsOfDate: str) -> Dict[str, Any]:
    """
    Core orchestrator entrypoint. Returns the same schema your UI expects.
    """
    logger.info(f"Orchestrator: handle_query: {query} | property={propertyCode} | AsOfDate={AsOfDate}")

    agents = simple_route(query)
    results: List[Dict[str, Any]] = []

    for a in agents:
        try:
            if a == "annual_summary":
                r = annual_agent(query, propertyCode, AsOfDate, force_broaden=False)
                results.append(r)
        except Exception:
            logger.exception(f"Orchestrator: Agent {a} failed")

    agg = _aggregate(results)

    # Fallback: if insufficient or low confidence, try forced broadening
    if (not agg or agg.get("requires_review") or agg.get("confidence", 0) < 0.4
        or "insufficient data" in (agg.get("answer_text", "").lower())):
        logger.warning("Orchestrator: primary pass insufficient, using broadened fallback")
        try:
            fb = annual_agent(query + " (broaden search)", propertyCode, AsOfDate, force_broaden=True)
            agg = _aggregate([fb] + results)
        except Exception:
            logger.exception("Orchestrator: Broadened fallback failed")

    return agg
