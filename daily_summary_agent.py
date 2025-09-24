# src/agents/daily_summary_agent.py
import json
from typing import Any, Dict, Optional

from common import llm, chroma, SYSTEM_PREFIX, JSON_SCHEMA_INSTRUCTION
from db_utils import fetch_one
from langchain.schema import HumanMessage

SYSTEM = SYSTEM_PREFIX + " Scope: daily snapshot and KPI explanations."


def fetch_daily_snapshot() -> Optional[Dict[str, Any]]:
    """Fetch a daily snapshot row for a given date."""
    query = f"""SELECT * FROM dailydata_transaction WHERE "Dates" between '2025-09-01' and '2025-09-30' and "AsOfDate" = '2025-09-21'"""
    return fetch_one(query)


def agent_handle(user_question, **kwargs):
    row = fetch_daily_snapshot()
    if not row:
        return {
            "answer_text":"insufficient data",
            "kpis":[],
            "explanations":[],
            "confidence":0.0,
            "sources":[],
            "suggested_actions":[]
        }
    evidence = f"""date:{row["Dates"]}|occupied:{row.get('"RoomSold"')}|available:{row.get('"RoomsOfAvailable"')}|room_rev:{row.get('"TotalRevenue"')}|adr:{row.get('"ADR"')}"""
    retrieved = chroma.similarity_search_with_score(user_question, k=3)
    docs_text = "\n\n---\n\n".join([f"source:{d.metadata.get('source_id','unknown')}\n{d.page_content}" for d, score in retrieved])
    prompt = f"{SYSTEM}\n\nEVIDENCE:\n{evidence}\n\nRETRIEVED_DOCS:\n{docs_text}\n\nUSER_QUESTION:\n{user_question}\n\n{JSON_SCHEMA_INSTRUCTION}\nProvide only valid JSON."
    resp = llm.generate([[HumanMessage(content=prompt)]])
    text = resp.generations[0][0].text
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = {"answer_text": text[:800], "kpis": [], "explanations": [], "confidence": 0.5, "sources": [], "suggested_actions": []}
    return parsed
