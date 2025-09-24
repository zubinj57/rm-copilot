# src/orchestrator.py
from typing import List, Dict, Any
import concurrent.futures

import daily_summary_agent


def simple_route(query: str) -> List:
    """
    Simple keyword-based router to determine which agents should handle the query.
    """
    q = query.lower()
    agents = []

    # if any(x in q for x in ["reservation", "who booked", "which booking"]):
    #     agents.append(reservation_agent)
    if any(x in q for x in ["revpar", "adr", "occupancy", "daily"]):
        agents.append(daily_summary_agent)
    # if any(x in q for x in ["forecast", "predict", "probability"]):
    #     agents.append(forecast_agent)
    # if any(x in q for x in ["pickup", "cumulative", "lead time"]):
    #     agents.append(pickup_agent)
    # if any(x in q for x in ["segment", "market segment", "corporate", "leisure"]):
    #     agents.append(market_segment_agent)
    # if any(x in q for x in ["rate plan", "rateplan", "bar", "package"]):
    #     agents.append(rate_plan_agent)
    # if any(x in q for x in ["pace", "booking pace", "lead days"]):
    #     agents.append(booking_pace_agent)

    # Default fallback if no agent matches
    if not agents:
        agents = [daily_summary_agent]

    return agents


def aggregate_results(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Aggregate responses from multiple agents into a single unified result.
    """
    agg = {
        "answer_text": "",
        "kpis": [],
        "explanations": [],
        "confidence": 0.0,
        "sources": [],
        "suggested_actions": [],
    }

    confidences = []
    answer_parts = []

    for r in results:
        if not r:
            continue
        answer_parts.append(r.get("answer_text", ""))
        agg["kpis"].extend(r.get("kpis", []))
        agg["explanations"].extend(r.get("explanations", []))
        agg["sources"].extend(r.get("sources", []))
        agg["suggested_actions"].extend(r.get("suggested_actions", []))
        confidences.append(r.get("confidence", 0.5))

    agg["answer_text"] = "\n\n".join([p for p in answer_parts if p])
    agg["confidence"] = sum(confidences) / len(confidences) if confidences else 0.0
    # Remove duplicates from sources
    agg["sources"] = list(dict.fromkeys(agg["sources"]))

    return agg


def handle_query(user_query, **kwargs):
    agent_modules = simple_route(user_query)
    print("------------------")
    print(agent_modules)
    results = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futures = []
        for m in agent_modules:
            futures.append(ex.submit(m.agent_handle, user_query, **kwargs))
        for f in concurrent.futures.as_completed(futures):
            results.append(f.result())
    aggregated = aggregate_results(results)
    if aggregated["confidence"] < 0.7:
        aggregated["requires_review"] = True
    return aggregated