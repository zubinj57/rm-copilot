from src.common import llm, SYSTEM_PREFIX, JSON_SCHEMA_INSTRUCTION, getChromaByPropertyCode
import json
from langchain.schema import HumanMessage
from datetime import datetime
import re

SYSTEM = SYSTEM_PREFIX + " Scope: annual summary KPIs, trends, and year-over-year performance."

MONTHS = {
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

def parse_year(text: str) -> str | None:
    """Extract a year (YYYY) from user text."""
    m = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    if m:
        return m.group(1)
    return None


def agent_handle(user_question, propertyCode: str = "", AsOfDate: str = "", year: str = ""):
    print("Start annual summary agent...")

    chroma = getChromaByPropertyCode(propertyCode, collection_name="annual_summary")

    flt = {"$and": [{"type": {"$eq": "annual_summary"}}]}
    if AsOfDate:
        flt["$and"].append({"AsOfDate": {"$eq": AsOfDate}})
    if propertyCode:
        flt["$and"].append({"propertyCode": {"$eq": propertyCode}})
    if year:
        flt["$and"].append({"year": {"$eq": year}})

    # try to extract year from the user question
    asked_year = parse_year(user_question)
    if asked_year:
        flt["$and"].append({"year": {"$eq": asked_year}})

    retrieved = chroma.similarity_search_with_score(user_question, k=5, filter=flt)
    print(f"Retrieved {len(retrieved)} documents from Chroma.")

    for doc, score in retrieved:
        print("Score:", score)
        print("Content:", doc.page_content)
        print("Metadata:", doc.metadata)
        print("-" * 50)

    docs_text = "\n\n---\n\n".join(
        [f"source:{d.page_content}" for d, score in retrieved]
    )

    prompt = (
        f"{SYSTEM}\n\n"
        f"RETRIEVED_DOCS:\n{docs_text}\n\n"
        f"USER_QUESTION:\n{user_question}\n\n"
        f"{JSON_SCHEMA_INSTRUCTION}\nProvide only valid JSON."
    )

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