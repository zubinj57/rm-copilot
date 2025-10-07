# src/app.py
from fastapi import FastAPI, HTTPException, Query
from typing import Dict, Any, Literal, Optional
from pydantic import BaseModel
import src.chroma_ingest as chroma_ingest
from src.db_config import get_db_connection
import sys
import os

from src.orchestrator import handle_query

# Add the parent directory to sys.path to allow importing utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.logger import get_custom_logger

logger = get_custom_logger(name="app_logger") 

app = FastAPI()

AllowedIngestType = Literal["daily_summary", "reservation", "performance_monitor", "annual_summary"]

class QueryRequest(BaseModel):
    query: str
    property_code: str
    as_of_date: str
    # params: Dict[str, Any] = {}

@app.get("/query")
async def query_endpoint_get(
    property_code: str = Query(..., description="Hotel property code"),
    as_of_date: str = Query(..., description="As Of Date (YYYY-MM-DD)"),
    q: str = Query(..., description="Query to execute"),
):
    return handle_query(q, property_code, as_of_date)

# ---------------------------
# POST endpoint (JSON body)
# ---------------------------
@app.post("/query")
async def query_endpoint_post(req: QueryRequest):
    return handle_query(req.query, req.property_code, req.as_of_date)

@app.get("/ingest")
async def ingest_endpoint(
    type: AllowedIngestType = Query(..., description="Ingest type"),
    property_code: str = Query(...),
    as_of_date: str = Query(..., pattern=r"^\d{4}-\d{2}-\d{2}$"),  # pattern instead of regex
    property_id: str = Query(...),
    client_id: str = Query(...),
    year: Optional[str] = Query(None),
):
    """
    Trigger ingestion of data into Chroma vector DB.
    Returns the number of documents ingested.
    """
    logger.info(f"Received ingestion request: type={type}, property_code={property_code}, as_of_date={as_of_date}, property_id={property_id}, client_id={client_id}, year={year}")
    conn = None
    try:
        conn = get_db_connection(PROPERTY_DATABASE=property_code, clientId=client_id)

        # Safe dynamic dispatch
        ingest_fn = getattr(chroma_ingest, f"ingest_{type}", None)
        if not callable(ingest_fn):
            raise HTTPException(status_code=400, detail=f"Ingestion function for type '{type}' not found.")

        inserted_count = ingest_fn(
            collection_name=type,
            PROPERTY_ID=property_id,
            PROPERTY_CODE=property_code,
            AS_OF_DATE=as_of_date,
            CLIENT_ID=client_id,
            conn=conn,
        )

        # Prefer success + count, don’t misuse 500s for “no data”
        return {
            "status": "success",
            "type": type,
            "property_code": property_code,
            "as_of_date": as_of_date,
            "documents_ingested": int(inserted_count or 0),
        }

    except HTTPException:
        # re-raise FastAPI HTTP errors
        raise
    except Exception as e:
        # unexpected server-side problem
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {e}")
    finally:
        # ensure connection is closed
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass

@app.get("/zubin")
async def welcome_rm():
    return {"msg": "Welcome to the RM Copilot."}


@app.get("/healthz")
def health_check():
    return {"status": "healthy"}
