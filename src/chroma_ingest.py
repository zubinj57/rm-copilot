# src/chroma_ingest.py
import os
import logging
import traceback
 
import pandas as pd
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from .common import getChromaByPropertyCode
from .document_list import daily_summaries_docs, reservation_docs, performance_monitor_docs, annual_summary_docs

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
# Ingestion Pipeline
# -----------------------------------------------------------------------------
def ingest_daily_summaries(propertyCode, AsOfDate) -> None:
    try:
 
        chroma = getChromaByPropertyCode(propertyCode,collection_name="daily_summaries")
 
        # Daily summaries
        docs, metadatas, ids = daily_summaries_docs(propertyCode, AsOfDate)
        if docs:
            texts = [doc.page_content for doc in docs]
            print(f"[Ingest] Ingesting {len(texts)} docs...")
            chroma.add_texts(texts=texts, metadatas=metadatas, ids=ids)
            print("[Count] Docs in collection:", chroma._collection.count())
            return chroma._collection.count()
        else:
            print("[Ingest] No daily summaries found to ingest.")
            return 0
           
    except Exception as e:
        logging.error("Error during ingestion: %s", e)
        traceback.print_exc()
        return 0
            
def ingest_reservation(propertyCode, AsOfDate) -> None:
    try:
 
        chroma = getChromaByPropertyCode(propertyCode,collection_name="reservations")
 
        # Daily summaries
        docs, metadatas, ids = reservation_docs(propertyCode, AsOfDate)
        if docs:
            texts = [doc.page_content for doc in docs]
            print(f"[Ingest] Ingesting {len(texts)} docs...")
            chroma.add_texts(texts=texts, metadatas=metadatas, ids=ids)
            print("[Count] Docs in collection:", chroma._collection.count())
            return chroma._collection.count()
        else:
            print("[Ingest] No daily summaries found to ingest.")
            return 0
           
    except Exception as e:
        logging.error("Error during ingestion: %s", e)
        traceback.print_exc()
        return 0

def ingest_performance_monitor(PROPERTY_ID="", PROPERTY_CODE="", AS_OF_DATE="", CLIENT_ID="",conn= None) -> None:
    try:
 
        chroma = getChromaByPropertyCode(PROPERTY_CODE,collection_name="performance_monitor")
 
        # Daily summaries
        docs, metadatas, ids = performance_monitor_docs(PROPERTY_ID=PROPERTY_ID, PROPERTY_CODE=PROPERTY_CODE, AS_OF_DATE=AS_OF_DATE, CLIENT_ID=CLIENT_ID, conn=conn)
        if docs:
            texts = [doc.page_content for doc in docs]
            print(f"[Ingest] Ingesting {len(texts)} docs...")
            chroma.add_texts(texts=texts, metadatas=metadatas, ids=ids)
            print("[Count] Docs in collection:", chroma._collection.count())
            return chroma._collection.count()
        else:
            print("[Ingest] No daily summaries found to ingest.")
            return 0
           
    except Exception as e:
        logging.error("Error during ingestion: %s", e)
        traceback.print_exc()
        return 0

def ingest_annual_summary(PROPERTY_ID="", PROPERTY_CODE="", AS_OF_DATE="", CLIENT_ID="",conn= None) -> None:
    try:
        chroma = getChromaByPropertyCode(PROPERTY_CODE, collection_name="annual_summary")

        # Annual summary docs
        docs, metadatas, ids = annual_summary_docs(PROPERTY_ID=PROPERTY_ID, PROPERTY_CODE=PROPERTY_CODE, AS_OF_DATE=AS_OF_DATE, CLIENT_ID=CLIENT_ID, conn=conn)

        print("DEBUG annual_summary_docs output count:", len(docs))
        print("DEBUG first doc sample:", docs[0].page_content if docs else "No docs")
        
        if docs:
            texts = [doc.page_content for doc in docs]
            print(f"[Ingest] Ingesting {len(texts)} annual summary docs...")
            chroma.add_texts(texts=texts, metadatas=metadatas, ids=ids)
            print("[Count] Docs in collection:", chroma._collection.count())
            return chroma._collection.count()
        else:
            print("[Ingest] No annual summaries found to ingest.")
            return 0
        
    except Exception as e:
        logging.error("Error during annual summary ingestion: %s", e)
        traceback.print_exc()
        return 0
    
