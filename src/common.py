# src/agents/common.py
import os
import traceback
from utils.logger import get_custom_logger
from datetime import datetime, timedelta, timezone
from typing import Tuple
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_chroma import Chroma
 
load_dotenv()
 
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
CHROMA_DIR = os.getenv("CHROMA_PERSIST_DIR", "./chroma_db")

if not OPENAI_KEY:
    raise ValueError("OPENAI_API_KEY is not set in environment variables.")
 
logger = get_custom_logger(name= "Common")
llm = ChatOpenAI(model="gpt-4", temperature=0.0, api_key=OPENAI_KEY)
emb = OpenAIEmbeddings(api_key=OPENAI_KEY)
 
 
SYSTEM_PREFIX = (
    "You are a specialist agent for a hotel revenue management system. "
    "Follow instructions precisely and return JSON according to the schema. "
    "Do not hallucinate; if the requested data is missing, return `answer_text` "
    "as `insufficient data` and confidence 0.0."
)
 
JSON_SCHEMA_INSTRUCTION = (
    "Return only valid JSON with these keys: "
    "answer_text (string), "
    "kpis (list of {name, value, unit, source}), "
    "explanations (list of {factor, impact_percent, evidence}), "
    "confidence (0.0-1.0), "
    "sources (list of strings), "
    "suggested_actions (list of strings)."
)
 
def getChromaByPropertyCode(propertyCode: str, collection_name: str = "default_collection") -> Chroma:
    chroma = Chroma(
        collection_name=collection_name,
        persist_directory=propertyCode,
        embedding_function=emb,
    )
    return chroma

# -----------------------------------------------------------------------------
# Defining Cutoff Date for Deletion and Ingestion
# -----------------------------------------------------------------------------
def cutoff_ts(as_of_date: str, days: int = 7) -> Tuple[int, datetime, int]:
    """Return UNIX timestamp (UTC) for (as_of_date - days)."""
    asof_dt = datetime.strptime(as_of_date, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    cutoff_dt = asof_dt - timedelta(days=days)
    return int(asof_dt.timestamp()), cutoff_dt, int(cutoff_dt.timestamp())



# -----------------------------------------------------------------------------
# Deletion Function
# -----------------------------------------------------------------------------
def delete_vectors(propertyCode: str, AsOfDate: str, chroma: Chroma ) -> None:
    try:
        collection = chroma._collection
        asof_timestamp, cutoff_date, cutoff_timestamp = cutoff_ts(AsOfDate, days=7)
        logger.info(f"Documents in collection: {collection.count()}")
        logger.info(f"Deleting documents older than {cutoff_date} from collection {collection.name}")
        res = collection.get(
            where={
                "$and": [
                    {"property_code": {"$eq": propertyCode}},
                    {"as_of_date_timestamp": {"$lte": cutoff_timestamp}},
                ]
            },
            include=["metadatas"],
        )
        victims = res.get("ids", []) or []
        
        if victims:
            logger.info(f"Found {len(victims)} documents to delete")
            collection.delete(ids=victims)
        else:
            logger.info("No documents to delete")

        logger.info(f"Deleted documents older than {cutoff_date} from collection {collection.name}")
        logger.info(f"Documents in collection after deletion: {collection.count()}")

        logger.info(f"Deleting Same day documents for {AsOfDate} from collection {collection.name}")
        res_same_day = collection.get(
            where={
                "$and": [
                    {"property_code": {"$eq": propertyCode}},
                    {"as_of_date_timestamp": {"$eq": asof_timestamp}},
                ]
            },
            include=["metadatas"],
        )
        same_day_victims = res_same_day.get("ids", []) or []
        
        if same_day_victims:
            logger.info(f"Found {len(same_day_victims)} same day documents to delete.")
            collection.delete(ids=same_day_victims)
        else:
            logger.info("No same day documents to delete")

    except Exception as e:
        logger.error("Error during deletion: %s", e)
        traceback.print_exc()

# -----------------------------------------------------------------------------
# Ingestion Function
# -----------------------------------------------------------------------------
def ingest_vectors(chroma: Chroma,
                   docs_meta_ids: tuple) -> int:
    try:
        docs, metadatas, ids = docs_meta_ids
        if docs:
            texts = [doc.page_content for doc in docs]
            logger.info(f"Ingesting {len(texts)} docs for {chroma._collection.name} ")
            chroma.add_texts(texts=texts, metadatas=metadatas, ids=ids)
            logger.info(f"Docs in collection after ingestion: {chroma._collection.count()}")
            return chroma._collection.count()
        else:
            logger.info("No docs found to ingest")
            return 0
    except Exception as e:
        logger.error("Error during ingestion: %s", e)
        traceback.print_exc()
        return 0
