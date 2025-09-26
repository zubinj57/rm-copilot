# src/app.py
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel
from typing import Dict, Any
import pandas as pd
from src.orchestrator import handle_query
from src.chroma_ingest import (ingest_daily_summaries, ingest_reservation, ingest_performance_monitor, ingest_annual_summary,)
from src.db_config import get_db_connection
app = FastAPI()


class QueryRequest(BaseModel):
    query: str
    params: Dict[str, Any] = {}


@app.get("/query")
async def query_endpoint(
    propertyCode: str = Query(..., description="Hotel property code"),
    AsOfDate: str = Query(..., description="As Of Date (YYYY-MM-DD)"),
    q: str = Query(..., description="Query to execute")
):
    result = handle_query(q, propertyCode, AsOfDate)
    return result


@app.get("/ingest")
async def ingest_endpoint(
    type: str = Query(..., regex="^(daily_summary|reservation|performance_monitor|annual_summary)$"),
    PROPERTY_CODE: str = Query(),
    AS_OF_DATE: str = Query(),
    PROPERTY_ID: str = Query(),
    CLIENT_ID: str = Query(),
    year: str = Query(None),
):
    """
    Trigger ingestion of data into Chroma vector DB.
    Supports ingestion of daily summaries or reservations.
    Returns the number of documents ingested.
    """
    inserted_count = 0
    config_db_conn = get_db_connection(PROPERTY_DATABASE=PROPERTY_CODE, clientId=CLIENT_ID)
    try:
        if type == "performance_monitor":
            inserted_count = ingest_performance_monitor(PROPERTY_ID=PROPERTY_ID, PROPERTY_CODE=PROPERTY_CODE, AS_OF_DATE=AS_OF_DATE, CLIENT_ID=CLIENT_ID, conn=config_db_conn)
        elif type == "annual_summary":
            if not year:
                year = str(pd.to_datetime(AS_OF_DATE).year)
            inserted_count = ingest_annual_summary(PROPERTY_ID=PROPERTY_ID, PROPERTY_CODE=PROPERTY_CODE, AS_OF_DATE=AS_OF_DATE, CLIENT_ID=CLIENT_ID, conn=config_db_conn)
        else:
            raise HTTPException(status_code=400, detail="Invalid type provided.")

        
        if inserted_count < 0:
            raise HTTPException(status_code=500, detail=f"Ingestion failed for {type}")

       # ❌ Fail if no documents inserted or negative count
        if inserted_count <= 0:
            raise HTTPException(
                status_code=500,
                detail=f"Ingestion failed for {type}. Inserted count: {inserted_count}"
            )

        return {
            "status": "success",
            "type": type,
            "documents_ingested": inserted_count
        }

    except Exception as e:
        return {"status": "fail", "msg": str(e)}
        

@app.get("/")
async def welcom_rm():

    return {"msg": "Welcome to the RM Copilot."}

# Run the server with:
# uvicorn app:app --reload --port 8000
