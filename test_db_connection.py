# test_db_connection.py
from src.db_utils import fetch_all, fetch_one

print("✅ Starting DB connection test...")

# 1️⃣ Check current time
try:
    result = fetch_one("SELECT NOW() as current_time;")
    print("🕒 Database time:", result)
except Exception as e:
    print("❌ Connection test failed:", e)

# 2️⃣ Check some table exists (example: dailydata_transaction)
try:
    rows = fetch_all("""
        SELECT "propertyCode", COUNT(*) AS total_rows
        FROM dailydata_transaction
        GROUP BY "propertyCode"
        LIMIT 5;
    """)
    print("🏨 Sample data:", rows)
except Exception as e:
    print("⚠️ Table check failed:", e)