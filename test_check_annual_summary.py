# test_check_annual_summary.py
from src.db_utils import fetch_all

property_code = "AC32AW"
as_of_date = "2025-10-09"

rows = fetch_all("""
    SELECT "propertyCode", "AsOfDate", COUNT(*) AS recs
    FROM dailydata_transaction
    WHERE "propertyCode" = %s
      AND "AsOfDate" = %s
    GROUP BY "propertyCode", "AsOfDate";
""", (property_code, as_of_date))

print(rows)
