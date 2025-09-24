# src/app.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from orchestrator import handle_query
from typing import Dict, Any
from chroma_ingest import ingest

app = FastAPI()


class QueryRequest(BaseModel):
    query: str
    params: Dict[str, Any] = {}


@app.post("/query")
async def query_endpoint(req: QueryRequest):
    if not req.query:
        raise HTTPException(status_code=400, detail="query is required")
    print(req.query)
    result = handle_query(req.query, **req.params)
    return result

@app.get("/status")
async def query_endpoint():
    return {"status": "ok"}

@app.post("/ingest")
async def ingest_endpoint():
    """
    Trigger ingestion of daily summaries into Chroma vector DB.
    Returns the number of documents ingested.
    """
    property_code = 'AC32AW'
    as_of_date = '2025-09-22'
    try:
        count = ingest(property_code,as_of_date)
        return {"status": "success", "documents_ingested": count}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Run the server with:
# uvicorn src.app:app --reload --port 8000
