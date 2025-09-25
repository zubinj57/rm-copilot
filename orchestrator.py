import daily_summary_agent
import concurrent.futures
 
def simple_route(query):
    q = query.lower()
    agents = []
    if any(x in q for x in ["revpar","adr","occupancy","daily"]):
        agents.append(daily_summary_agent)
    if not agents:
    # default to daily + forecast
        agents = [daily_summary_agent]
    return agents
 
def aggregate_results(results: list[dict]) -> dict:
    """Aggregate results from multiple agents into a single response."""
    agg = {
        "answer_text": "",
        "kpis": [],
        "explanations": [],
        "confidence": 0.0,
        "sources": [],
        "suggested_actions": [],
    }
 
    confidences = []
    parts = []
 
    for r in results:
        if not r:
            continue
        parts.append(r.get("answer_text", ""))
        agg["kpis"].extend(r.get("kpis", []))
        agg["explanations"].extend(r.get("explanations", []))
        agg["sources"].extend(r.get("sources", []))
        agg["suggested_actions"].extend(r.get("suggested_actions", []))
        confidences.append(r.get("confidence", 0.5))
 
    agg["answer_text"] = "\n\n".join([p for p in parts if p])
    agg["confidence"] = sum(confidences) / len(confidences) if confidences else 0.0
    agg["sources"] = list(dict.fromkeys(agg["sources"]))  # deduplicate
 
    return agg
 
def handle_query(user_query: str, propertyCode: str, AsOfDate: str, **kwargs) -> dict:
   
    user_query = user_query.strip()
 
 
    agent_modules = simple_route(user_query)
    print(agent_modules)
    results = []
 
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
        futures = [ex.submit(m.agent_handle, user_query, propertyCode, AsOfDate, **kwargs) for m in agent_modules]
        for f in concurrent.futures.as_completed(futures):
            print(f)
            results.append(f.result())
 
    print(results)
    aggregated = aggregate_results(results)
    print(aggregated)
 
    if aggregated["confidence"] < 0.7:
        aggregated["requires_review"] = True
 
    return aggregated
 