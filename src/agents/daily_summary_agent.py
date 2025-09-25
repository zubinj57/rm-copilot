# src/agents/daily_summary_agent.py
from src.common import llm, SYSTEM_PREFIX, JSON_SCHEMA_INSTRUCTION, getChromaByPropertyCode
from src.db_utils import fetch_one
import json
from langchain.schema import HumanMessage
from datetime import datetime
import re
 
SYSTEM = SYSTEM_PREFIX + " Scope: daily snapshot and KPI explanations."
 
 
def fetch_daily_snapshot(date):
    q = f"""SELECT * FROM dailydata_transaction WHERE "Dates" = %s"""
    return fetch_one(q, (date,))

MONTHS = {
    "jan":1,"january":1, "feb":2,"february":2, "mar":3,"march":3, "apr":4,"april":4,
    "may":5, "jun":6,"june":6, "jul":7,"july":7, "aug":8,"august":8,
    "sep":9,"sept":9,"september":9, "oct":10,"october":10, "nov":11,"november":11,
    "dec":12,"december":12,
}

def parse_staydate(text: str) -> str | None:
    s = text.lower().replace(",", " ")
    s = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", s)  # 23rd -> 23

    # ISO: 2025-12-23
    m = re.search(r"\b(\d{4})-(\d{1,2})-(\d{1,2})\b", s)
    if m:
        y, mo, d = map(int, m.groups())
        return f"{y:04d}-{mo:02d}-{d:02d}"

    # Year Month Day: 2025 Dec 23
    m = re.search(r"\b(\d{4})\s+([a-z]{3,9})\s+(\d{1,2})\b", s)
    if m:
        y, mon, d = m.groups()
        mo = MONTHS.get(mon, MONTHS.get(mon[:3], 0))
        if mo:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    # Day Month Year: 23 Dec 2025
    m = re.search(r"\b(\d{1,2})\s+([a-z]{3,9})\s+(\d{4})\b", s)
    if m:
        d, mon, y = m.groups()
        mo = MONTHS.get(mon, MONTHS.get(mon[:3], 0))
        if mo:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"

    # Month Day Year: Dec 23 2025
    m = re.search(r"\b([a-z]{3,9})\s+(\d{1,2})\s+(\d{4})\b", s)
    if m:
        mon, d, y = m.groups()
        mo = MONTHS.get(mon, MONTHS.get(mon[:3], 0))
        if mo:
            return f"{int(y):04d}-{int(mo):02d}-{int(d):02d}"
    return None

def agent_handle(user_question,propertyCode: str="", AsOfDate: str=""):
    print("Start agent script........")
    chroma = getChromaByPropertyCode(propertyCode,collection_name="daily_summaries")
 
    flt = {"$and": [{"type": {"$eq": "daily_summary"}}]}
    if AsOfDate:
        flt["$and"].append({"AsOfDate": {"$eq": AsOfDate}})
    if propertyCode:
        flt["$and"].append({"propertyCode": {"$eq": propertyCode}})

    asked_date = parse_staydate(user_question)
    if asked_date:
        flt["$and"].append({"Dates": {"$eq": asked_date}})   # <-- key line

    retrieved = chroma.similarity_search_with_score(user_question, k=10, filter=flt)
    if not retrieved and asked_date:
        from datetime import timedelta
        dt = datetime.strptime(asked_date, "%Y-%m-%d")
        near = [(dt + timedelta(days=o)).strftime("%Y-%m-%d") for o in (-1, 1)]
        flt_near = {"$and": [c for c in flt["$and"] if "Dates" not in c] + [{"Dates": {"$in": near}}]}
        retrieved = chroma.similarity_search_with_score(user_question, k=10, filter=flt_near)


    print(f"Retrieved {len(retrieved)} documents from Chroma.")
 
    for doc, score in retrieved:
        print("Score:", score)
        print("Content:", doc.page_content)
        print("Metadata:", doc.metadata)
        print("-" * 50)
 
 
    docs_text = "\n\n---\n\n".join(
        [
            f"source: {d.metadata.get('source_id', 'unknown')}\n{d.page_content}"
            for d, score in retrieved
        ]
    )
 
    docs_text = "\n\n---\n\n".join(
        [
            f"source:{d.page_content}"
            for d, score in retrieved
        ]
    )
    # print(docs_text)
    prompt = (
        f"{SYSTEM}\n\n"
        f"RETRIEVED_DOCS:\n{docs_text}\n\n"
        f"USER_QUESTION:\n{user_question}\n\n"
        f"{JSON_SCHEMA_INSTRUCTION}\nProvide only valid JSON."
    )
    # print("Prompt to LLM:", prompt)
    # resp = llm.generate([{"role": "user", "content": prompt}])
    resp = llm.generate([[HumanMessage(content=prompt)]])
    print("LLM raw response:", resp)
    text = resp.generations[0][0].message.content
 
    try:
        parsed = json.loads(text)
    except Exception:
        parsed = {
            "answer_text": text[:800],
            "kpis": [],
            "explanations": [],
            "confidence": 0.5,
            "sources": [],
            "suggested_actions": [],
        }
 
    return parsed