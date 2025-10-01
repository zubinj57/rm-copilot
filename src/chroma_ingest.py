# src/chroma_ingest.py
import os
import logging
import traceback
import pandas as pd

from datetime import datetime, timedelta
from dotenv import load_dotenv
from langchain_openai import OpenAIEmbeddings
from .common import getChromaByPropertyCode, delete_vectors, ingest_vectors
from .document_list import daily_summaries_docs, reservation_docs, performance_monitor_docs, docs_annual_summary
from utils.logger import get_custom_logger

# -----------------------------------------------------------------------------
# Config & Setup
# -----------------------------------------------------------------------------
load_dotenv()
 
# PERSIST_DIR: str = os.getenv("CHROMA_PERSIST_DIR", "./chroma_db")
OPENAI_KEY: str | None = os.getenv("OPENAI_API_KEY")
 

 
if not OPENAI_KEY:
    raise ValueError("OPENAI_API_KEY is not set in environment variables.")
 
embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_KEY)

logger = get_custom_logger(name= "Chroma Ingest")

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
            logger.info(f"Ingesting {len(texts)} docs")
            chroma.add_texts(texts=texts, metadatas=metadatas, ids=ids)
            logger.info("[Count] Docs in collection:", chroma._collection.count())
            return chroma._collection.count()
        else:
            print("[Ingest] No daily summaries found to ingest.")
            return 0
           
    except Exception as e:
        logger.error("Error during ingestion: %s", e)
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
        logger.error("Error during ingestion: %s", e)
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
        logger.error("Error during ingestion: %s", e)
        traceback.print_exc()
        return 0

def ingest_annual_summary(collection_name: str,
                          PROPERTY_ID: str = "", 
                          PROPERTY_CODE: str = "", 
                          AS_OF_DATE: str = "", 
                          CLIENT_ID:str ="",
                          conn= None) -> None:
    try:
        logger.info(f"Starting {collection_name} ingestion for {PROPERTY_CODE} as of {AS_OF_DATE}")
        chroma = getChromaByPropertyCode(PROPERTY_CODE, collection_name=collection_name)
        delete_vectors(PROPERTY_CODE, AS_OF_DATE, chroma)
        ingest_vectors(
            chroma,
            docs_annual_summary(
                PROPERTY_ID=PROPERTY_ID,
                PROPERTY_CODE=PROPERTY_CODE,
                AS_OF_DATE=AS_OF_DATE,
                CLIENT_ID=CLIENT_ID,
                conn=conn
            )
        )
        return int(chroma._collection.count())
    

    except Exception as e:
        logger.error("Error during annual summary ingestion: %s", e)
        traceback.print_exc()
        return 0
    
