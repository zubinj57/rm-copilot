# src/chroma_ingest.py
import os
import logging
import traceback
from typing import List, Tuple, Dict, Any
 
import pandas as pd
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain.schema import Document
from sqlalchemy import create_engine
 
 
 
# -----------------------------------------------------------------------------
# Config & Setup
# -----------------------------------------------------------------------------
load_dotenv()
 
# PERSIST_DIR: str = os.getenv("CHROMA_PERSIST_DIR", "./chroma_db")
OPENAI_KEY: str | None = os.getenv("OPENAI_API_KEY")
 
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
 
if not OPENAI_KEY:
    raise ValueError("OPENAI_API_KEY is not set in environment variables.")
 
embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_KEY)
 
 
# -----------------------------------------------------------------------------
# Data Fetching
# -----------------------------------------------------------------------------
def fetch_daily_summaries(propertyCode, AsOfDate) -> pd.DataFrame:
    """Fetch latest daily summaries from Postgres."""
    q =f"""
   SELECT
    "AsOfDate",
    "Dates",
    "Inventory",
    "RoomSold",
    "TotalRevenue",
    "ADR",
    "AvailableOccupancy",
    "RevPAR",
    "Occperc",
    "OutOfOrder",
    "RoomsOfAvailable",
    "DayOfWeek",
    "WeekType",
    "GroupADR",
    "GroupBlock",
    "GroupOTB",
    "GroupRevenue",
    "TransientRoomSold",
    "TransientRevenue",
    "TransientADR",
    "LYTotalInventory",
    "LYTotalRoomSold",
    "LYTotalRevenue",
    "LYTotalADR",
    "LYTotalOccupancy",
    "LYTotalRevPar",
    "LYTotalOccPerc",
    "LYPaceInventory",
    "LYPaceRoomSold",
    "LYPaceRevenue",
    "LYPaceADR",
    "LYPaceOccupancy",
    "LYPaceRevPar",
    "LYPaceOccPerc"
FROM dailydata_transaction
WHERE
"propertyCode" = '{propertyCode}'
and "AsOfDate" = '{AsOfDate}'
and "Dates" BETWEEN (CURRENT_DATE - INTERVAL '1 month')
                    AND (CURRENT_DATE + INTERVAL '3 month')
ORDER BY "AsOfDate" DESC;"""
 
 
 
    engine = create_engine("postgresql+psycopg2://postgres:9MGPMPiDn2RdegC2QMhc@backup-db-ema-postgres.cryru6bacdry.us-east-1.rds.amazonaws.com:5432/AC32AW")
    with engine.connect() as conn:
        return pd.read_sql(q, conn)
 
 
# -----------------------------------------------------------------------------
# Document Preparation
# -----------------------------------------------------------------------------
def daily_summaries_docs(propertyCode, AsOfDate) -> Tuple[List[Document], List[Dict[str, Any]], List[str]]:
    """Convert daily summaries into Document objects, metadata, and IDs."""
    df = fetch_daily_summaries(propertyCode, AsOfDate)
 
    docs: List[Document] = []
    metadatas: List[Dict[str, Any]] = []
    ids: List[str] = []
 
    for _, row in df.iterrows():
        text = (
            f"Daily Summary Snapshot date: {row['AsOfDate']} "
            f"(Dates: {row['Dates']}, {row['DayOfWeek']}, WeekType: {row['WeekType']})\n\n"
           
            f" Current Year:\n"
            f"- Inventory: {row['Inventory']}\n"
            f"- Rooms Sold: {row['RoomSold']}\n"
            f"- Total Revenue: {row['TotalRevenue']}\n"
            f"- ADR: {row['ADR']}\n"
            f"- Occupancy %: {row['Occperc']}\n"
            f"- RevPAR: {row['RevPAR']}\n"
            f"- Out Of Order: {row['OutOfOrder']}\n"
            f"- Rooms Available: {row['RoomsOfAvailable']}\n"
            f"- Group ADR: {row['GroupADR']}, "
            f"Group Revenue: {row['GroupRevenue']}, "
            f"Group OTB: {row['GroupOTB']}\n"
            f"- Transient Rooms Sold: {row['TransientRoomSold']}, "
            f"Transient ADR: {row['TransientADR']}, "
            f"Transient Revenue: {row['TransientRevenue']}\n\n"
           
            f" Last Year (same date):\n"
            f"- Inventory: {row['LYTotalInventory']}\n"
            f"- Rooms Sold: {row['LYTotalRoomSold']}\n"
            f"- Total Revenue: {row['LYTotalRevenue']}\n"
            f"- ADR: {row['LYTotalADR']}\n"
            f"- Occupancy %: {row['LYTotalOccPerc']}\n"
            f"- RevPAR: {row['LYTotalRevPar']}\n\n"
           
            f" Last Year (same time pace):\n"
            f"- Inventory: {row['LYPaceInventory']}\n"
            f"- Rooms Sold: {row['LYPaceRoomSold']}\n"
            f"- Total Revenue: {row['LYPaceRevenue']}\n"
            f"- ADR: {row['LYPaceADR']}\n"
            f"- Occupancy %: {row['LYPaceOccPerc']}\n"
            f"- RevPAR: {row['LYPaceRevPar']}"
        )
 
        # Create Document
        docs.append(Document(page_content=text))
 
        # Metadata
        metadatas.append({
            "propertyCode": propertyCode,
            "AsOfDate": row["AsOfDate"].strftime("%Y-%m-%d"),
            "Dates": row["Dates"].strftime("%Y-%m-%d"),
            "DayOfWeek": row["DayOfWeek"],
            "WeekType": row["WeekType"],
            "type": "daily_summary",
            "LYTotalInventory": row["LYTotalInventory"],
            "LYTotalRoomSold": row["LYTotalRoomSold"],
            "LYTotalRevenue": row["LYTotalRevenue"],
            "LYTotalADR": row["LYTotalADR"],
            "LYTotalOccupancy": row["LYTotalOccupancy"],
            "LYTotalRevPar": row["LYTotalRevPar"],
            "LYTotalOccPerc": row["LYTotalOccPerc"],
            "LYPaceInventory": row["LYPaceInventory"],
            "LYPaceRoomSold": row["LYPaceRoomSold"],
            "LYPaceRevenue": row["LYPaceRevenue"],
            "LYPaceADR": row["LYPaceADR"],
            "LYPaceOccupancy": row["LYPaceOccupancy"],
            "LYPaceRevPar": row["LYPaceRevPar"],
            "LYPaceOccPerc": row["LYPaceOccPerc"],
        })
 
        # Unique ID
        ids.append(f"daily_summary_{row['Dates']}")
 
    return docs, metadatas, ids
 
# -----------------------------------------------------------------------------
# Ingestion Pipeline
# -----------------------------------------------------------------------------
def ingest_daily_summaries(propertyCode, AsOfDate) -> None:
    try:
        import os
 
        # Show where we're writing to
        abs_dir = os.path.abspath(propertyCode)
        print(f"[Chroma] persist_directory: {abs_dir}")
 
        chroma = Chroma(
            collection_name="daily_summaries",
            persist_directory=propertyCode,       # this enables on-disk persistence
            embedding_function=embeddings,
        )
 
        # Daily summaries
        docs, metadatas, ids = daily_summaries_docs(propertyCode, AsOfDate)
        if docs:
            texts = [doc.page_content for doc in docs]
            print(f"[Ingest] Ingesting {len(texts)} docs...")
            chroma.add_texts(texts=texts, metadatas=metadatas, ids=ids)
        else:
            print("[Ingest] No daily summaries found to ingest.")
            return
 
        try:
            print("[Count] Docs in collection:", chroma._collection.count())
        except Exception:
            print("[Count] Could not read collection count.")
           
    except Exception as e:
        logging.error("Error during ingestion: %s", e)
        traceback.print_exc()
 
 
# -----------------------------------------------------------------------------
# Entry Point
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    propertyCode = "AC32AW"
    AsOfDate = "2025-09-23"
    ingest_daily_summaries(propertyCode, AsOfDate)
 
 