# src/chroma_ingest.py
import os
import pandas as pd
from dotenv import load_dotenv
from typing import List, Tuple, Dict, Any

from langchain_openai import OpenAIEmbeddings
from langchain_chroma import Chroma
from langchain.schema import Document
from db_utils import get_pg_conn

load_dotenv()

PERSIST_DIR = os.getenv("CHROMA_PERSIST_DIR", "./chroma_db")
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_KEY)




def fetch_daily_summaries(property_code,as_of_date) -> pd.DataFrame:
    """Fetch latest daily summaries from Postgres."""
    with get_pg_conn() as conn:
        query = f"""
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
        "propertyCode" = '{property_code}'
        and "AsOfDate" = '{as_of_date}'
        and "Dates" BETWEEN (CURRENT_DATE - INTERVAL '1 month') 
                            AND (CURRENT_DATE + INTERVAL '3 month')
        ORDER BY "AsOfDate" DESC;
        """
        return pd.read_sql(query, conn)


def daily_summaries_docs(property_code,as_of_date) -> Tuple[List[Document], List[Dict[str, Any]], List[str]]:
    """Convert daily summaries into Document objects, metadata, and IDs."""
    df = fetch_daily_summaries(property_code,as_of_date)
    print(df)

    docs: List[Document] = []
    metadatas: List[Dict[str, Any]] = []
    ids: List[str] = []

    for _, row in df.iterrows():
        text = (
            f"Daily Summary for {row['AsOfDate']} "
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
            "AsOfDate": str(row["AsOfDate"]),
            "Dates": str(row["Dates"]),
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


def ingest(property_code,as_of_date) -> int:
    """Ingest all supported document types into Chroma."""
    try:
        chroma = Chroma(persist_directory=PERSIST_DIR, embedding_function=embeddings)

        # Daily summaries
        docs, metadatas, ids = daily_summaries_docs(property_code,as_of_date)
        if docs: 
            # Precompute embeddings
            texts = [doc.page_content for doc in docs if doc.page_content.strip()]
            embeddings_list = embeddings.embed_documents(texts)

            batch_size = 50
            for i in range(0, len(texts), batch_size):
                batch_texts = texts[i:i+batch_size]
                batch_embs = embeddings_list[i:i+batch_size]
                batch_metadatas = metadatas[i:i+batch_size] if metadatas else None
                batch_ids = ids[i:i+batch_size] if ids else None
                print(f"Adding batch {i}-{i+len(batch_texts)}")
                chroma.add_texts(
                    texts=batch_texts,
                    metadatas=batch_metadatas,
                    ids=batch_ids,
                    embeddings=batch_embs  # pass precomputed embeddings
                )

        # TODO: Add other doc types (forecasts, FAQs, etc.)

        chroma.persist()

    except Exception as e:
        print(e)
        return 0
    finally:
        print("Finally calll")
        return len(docs)

