from src.db_config import get_db_connection
from sqlalchemy import text

# Example 1: Test with clientId (no property DB directly given)
try:
    conn = get_db_connection(PROPERTY_DATABASE='CHIZI', clientId=1) 
    result = conn.execute(text("SELECT NOW();"))   # works for both MySQL and Postgres
    print("✅ DB Connection Successful, Current Time:", result.scalar())
    conn.close()
except Exception as e:
    print("❌ DB Connection Failed:", str(e))


# Example 2: Test with property database
try:
    conn = get_db_connection(PROPERTY_DATABASE='CHIZI', clientId=1)  # replace with real property
    result = conn.execute(text("SELECT NOW();"))
    print("✅ Property DB Connection Successful, Current Time:", result.scalar())
    conn.close()
except Exception as e:
    print("❌ Property DB Connection Failed:", str(e))
