import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv


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
    conn = get_pg_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params or ())
        res = cur.fetchone()
    conn.close()
    return res

def fetch_all(query, params=None):
    conn = get_pg_conn()
    with conn.cursor(cursor_factory=RealDictCursor) as cur:
        cur.execute(query, params or ())
        res = cur.fetchall()
    conn.close()
    return res