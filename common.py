# src/agents/common.py
import os
from dotenv import load_dotenv
from langchain_openai import ChatOpenAI, OpenAIEmbeddings
from langchain_chroma import Chroma
 
load_dotenv()
 
OPENAI_KEY = os.getenv("OPENAI_API_KEY")
CHROMA_DIR = os.getenv("CHROMA_PERSIST_DIR", "./chroma_db")
 
if not OPENAI_KEY:
    raise ValueError("OPENAI_API_KEY is not set in environment variables.")
 
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
 
def getchromabypropertyCode(propertyCode: str, collection_name: str = "default_collection") -> Chroma:
    chroma = Chroma(
        collection_name=collection_name,
        persist_directory=propertyCode,
        embedding_function=emb,
    )
    return chroma
 