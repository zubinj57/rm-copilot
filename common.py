# src/agents/common.py
import os
from dotenv import load_dotenv
from langchain.chat_models import ChatOpenAI
from langchain.embeddings import OpenAIEmbeddings
from langchain.vectorstores import Chroma

load_dotenv()

OPENAI_KEY = os.getenv("OPENAI_API_KEY")
CHROMA_DIR = os.getenv("CHROMA_PERSIST_DIR", "./chroma_db")

llm = ChatOpenAI(
    model_name="gpt-4",
    temperature=0.0,
    openai_api_key=OPENAI_KEY,
)

embeddings = OpenAIEmbeddings(openai_api_key=OPENAI_KEY)

chroma = Chroma(
    persist_directory=CHROMA_DIR,
    embedding_function=embeddings,
)

SYSTEM_PREFIX = (
    "You are a specialist agent for a hotel revenue management system. "
    "Follow instructions precisely and return JSON according to the schema. "
    "Do not hallucinate; if the requested data is missing, return "
    "`answer_text` as `insufficient data` and confidence 0.0."
)

JSON_SCHEMA_INSTRUCTION = (
    "Return only valid JSON with these keys: "
    "answer_text (string), "
    "kpis (list of {name,value,unit,source}), "
    "explanations (list of {factor,impact_percent,evidence}), "
    "confidence (0.0-1.0), "
    "sources (list of strings), "
    "suggested_actions (list of strings)."
)
