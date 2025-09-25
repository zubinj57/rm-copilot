# demo_run.py
import os
from dotenv import load_dotenv
from src.orchestrator import handle_query
 
load_dotenv()
 
if __name__ == "__main__":
    propertyCode = "CHIZI"
    AsOfDate = "2025-09-23"
 
    q = f"""Give me a daily summary for 2025 Sep 25th including RevPAR, ADR, and Occupancy."""
    # q=f"""What is the ADR on 29th Sep 2025?"""
    out = handle_query(q,propertyCode, AsOfDate)
    print(out)
    import json
    print(json.dumps(out, indent=2))