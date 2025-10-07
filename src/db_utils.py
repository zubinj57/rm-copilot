import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
import traceback

load_dotenv()


def get_pg_conn():
    return psycopg2.connect(
    host=os.getenv("PG_HOST"),
    port=os.getenv("PG_PORT"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASS"),
    dbname=os.getenv("PG_DB"),
)

def fetch_one(query, params=None):
    """Fetch a single record as dict."""
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            print(f"Running SQL: {query}")
            if params:
                print(f"With params: {params}")
            cur.execute(query, params or ())
            res = cur.fetchone()
            return res
    except Exception as e:
        print("❌ [DB ERROR] fetch_one failed:", e)
        traceback.print_exc()
        raise
    finally:
        conn.close()

def fetch_all(query, params=None):
    """Fetch all records as list of dicts."""
    conn = get_pg_conn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            print(f"Running SQL: {query}")
            if params:
                print(f"With params: {params}")
            cur.execute(query, params or ())
            res = cur.fetchall()
            print(f"Rows returned: {len(res)}")
            return res
    except Exception as e:
        print("❌ [DB ERROR] fetch_all failed:", e)
        traceback.print_exc()
        raise
    finally:
        conn.close()