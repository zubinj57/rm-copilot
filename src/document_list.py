from typing import List, Tuple, Dict, Any
from langchain.schema import Document
from .data_layer import fetch_daily_summaries, fetch_reservation, get_PerformanceMonitor, get_annual_summary
from utils.month_normalizer import normalize_month_str, month_num_for_sort
import uuid
import pandas as pd
import calendar
import re
from datetime import datetime, timezone


# -----------------------------------------------------------------------------
# Document Preparation
# -----------------------------------------------------------------------------

def daily_summaries_docs(propertyCode, AsOfDate) -> Tuple[List[Document], List[Dict[str, Any]], List[str]]:
    """Convert daily summaries into Document objects, metadata, and IDs."""
    df = fetch_daily_summaries(propertyCode, AsOfDate)
 
    docs: List[Document] = []
    metadatas: List[Dict[str, Any]] = []
    ids: List[str] = []
 
    for _, row in df.iterrows():
        text = (
            f"Daily Summary Snapshot date: {row['AsOfDate']} "
            f"(Dates: {row['Dates']}, {row['DayOfWeek']}, WeekType: {row['WeekType']})\n\n"
           
            f" Current Year:\n"
            f"- Inventory: {row['Inventory']}\n"
            f"- Rooms Sold: {row['RoomSold']}\n"
            f"- Total Revenue: {row['TotalRevenue']}\n"
            f"- ADR: {row['ADR']}\n"
            f"- Occupancy %: {row['Occperc']}\n"
            f"- RevPAR: {row['RevPAR']}\n"
            f"- Out Of Order: {row['OutOfOrder']}\n"
            f"- Rooms Available: {row['RoomsOfAvailable']}\n"
            f"- Group ADR: {row['GroupADR']}, "
            f"Group Revenue: {row['GroupRevenue']}, "
            f"Group OTB: {row['GroupOTB']}\n"
            f"- Transient Rooms Sold: {row['TransientRoomSold']}, "
            f"Transient ADR: {row['TransientADR']}, "
            f"Transient Revenue: {row['TransientRevenue']}\n\n"
           
            f" Last Year (same date):\n"
            f"- Inventory: {row['LYTotalInventory']}\n"
            f"- Rooms Sold: {row['LYTotalRoomSold']}\n"
            f"- Total Revenue: {row['LYTotalRevenue']}\n"
            f"- ADR: {row['LYTotalADR']}\n"
            f"- Occupancy %: {row['LYTotalOccPerc']}\n"
            f"- RevPAR: {row['LYTotalRevPar']}\n\n"
           
            f" Last Year (same time pace):\n"
            f"- Inventory: {row['LYPaceInventory']}\n"
            f"- Rooms Sold: {row['LYPaceRoomSold']}\n"
            f"- Total Revenue: {row['LYPaceRevenue']}\n"
            f"- ADR: {row['LYPaceADR']}\n"
            f"- Occupancy %: {row['LYPaceOccPerc']}\n"
            f"- RevPAR: {row['LYPaceRevPar']}"
        )
 
        # Create Document
        docs.append(Document(page_content=text))
 
        # Metadata
        metadatas.append({
            "propertyCode": propertyCode,
            "AsOfDate": row["AsOfDate"].strftime("%Y-%m-%d"),
            "Dates": row["Dates"].strftime("%Y-%m-%d"),
            "DayOfWeek": row["DayOfWeek"],
            "WeekType": row["WeekType"],
            "type": "daily_summary",
            "LYTotalInventory": row["LYTotalInventory"],
            "LYTotalRoomSold": row["LYTotalRoomSold"],
            "LYTotalRevenue": row["LYTotalRevenue"],
            "LYTotalADR": row["LYTotalADR"],
            "LYTotalOccupancy": row["LYTotalOccupancy"],
            "LYTotalRevPar": row["LYTotalRevPar"],
            "LYTotalOccPerc": row["LYTotalOccPerc"],
            "LYPaceInventory": row["LYPaceInventory"],
            "LYPaceRoomSold": row["LYPaceRoomSold"],
            "LYPaceRevenue": row["LYPaceRevenue"],
            "LYPaceADR": row["LYPaceADR"],
            "LYPaceOccupancy": row["LYPaceOccupancy"],
            "LYPaceRevPar": row["LYPaceRevPar"],
            "LYPaceOccPerc": row["LYPaceOccPerc"],
        })
 
        # Unique ID
        ids.append(f"daily_summary_{row['Dates']}")
 
    return docs, metadatas, ids
 
def reservation_docs(propertyCode, AsOfDate) -> Tuple[List[Document], List[Dict[str, Any]], List[str]]:
    """Convert daily summaries into Document objects, metadata, and IDs."""
    df = fetch_reservation(propertyCode, AsOfDate)
 
    docs: List[Document] = []
    metadatas: List[Dict[str, Any]] = []
    ids: List[str] = []
 
    for _, row in df.iterrows():
        text = (
            f"Daily Summary Snapshot date: {row['AsOfDate']} "
            f"(Dates: {row['Dates']}, {row['DayOfWeek']}, WeekType: {row['WeekType']})\n\n"
           
            f" Current Year:\n"
            f"- Inventory: {row['Inventory']}\n"
            f"- Rooms Sold: {row['RoomSold']}\n"
            f"- Total Revenue: {row['TotalRevenue']}\n"
            f"- ADR: {row['ADR']}\n"
            f"- Occupancy %: {row['Occperc']}\n"
            f"- RevPAR: {row['RevPAR']}\n"
            f"- Out Of Order: {row['OutOfOrder']}\n"
            f"- Rooms Available: {row['RoomsOfAvailable']}\n"
            f"- Group ADR: {row['GroupADR']}, "
            f"Group Revenue: {row['GroupRevenue']}, "
            f"Group OTB: {row['GroupOTB']}\n"
            f"- Transient Rooms Sold: {row['TransientRoomSold']}, "
            f"Transient ADR: {row['TransientADR']}, "
            f"Transient Revenue: {row['TransientRevenue']}\n\n"
           
            f" Last Year (same date):\n"
            f"- Inventory: {row['LYTotalInventory']}\n"
            f"- Rooms Sold: {row['LYTotalRoomSold']}\n"
            f"- Total Revenue: {row['LYTotalRevenue']}\n"
            f"- ADR: {row['LYTotalADR']}\n"
            f"- Occupancy %: {row['LYTotalOccPerc']}\n"
            f"- RevPAR: {row['LYTotalRevPar']}\n\n"
           
            f" Last Year (same time pace):\n"
            f"- Inventory: {row['LYPaceInventory']}\n"
            f"- Rooms Sold: {row['LYPaceRoomSold']}\n"
            f"- Total Revenue: {row['LYPaceRevenue']}\n"
            f"- ADR: {row['LYPaceADR']}\n"
            f"- Occupancy %: {row['LYPaceOccPerc']}\n"
            f"- RevPAR: {row['LYPaceRevPar']}"
        )
 
        # Create Document
        docs.append(Document(page_content=text))
 
        # Metadata
        metadatas.append({
            "propertyCode": propertyCode,
            "AsOfDate": row["AsOfDate"].strftime("%Y-%m-%d"),
            "Dates": row["Dates"].strftime("%Y-%m-%d"),
            "DayOfWeek": row["DayOfWeek"],
            "WeekType": row["WeekType"],
            "type": "daily_summary",
            "LYTotalInventory": row["LYTotalInventory"],
            "LYTotalRoomSold": row["LYTotalRoomSold"],
            "LYTotalRevenue": row["LYTotalRevenue"],
            "LYTotalADR": row["LYTotalADR"],
            "LYTotalOccupancy": row["LYTotalOccupancy"],
            "LYTotalRevPar": row["LYTotalRevPar"],
            "LYTotalOccPerc": row["LYTotalOccPerc"],
            "LYPaceInventory": row["LYPaceInventory"],
            "LYPaceRoomSold": row["LYPaceRoomSold"],
            "LYPaceRevenue": row["LYPaceRevenue"],
            "LYPaceADR": row["LYPaceADR"],
            "LYPaceOccupancy": row["LYPaceOccupancy"],
            "LYPaceRevPar": row["LYPaceRevPar"],
            "LYPaceOccPerc": row["LYPaceOccPerc"],
        })
 
        # Unique ID
        ids.append(f"daily_summary_{row['Dates']}")
 
    return docs, metadatas, ids
 
def docs_performance_monitor(PROPERTY_ID="", PROPERTY_CODE="", AS_OF_DATE="", CLIENT_ID="",conn= None) -> Tuple[List[Document], List[Dict[str, Any]], List[str]]:
    """Convert daily summaries into Document objects, metadata, and IDs."""

    response_json, error_list = get_PerformanceMonitor(PROPERTY_ID=PROPERTY_ID, PROPERTY_CODE=PROPERTY_CODE, AS_OF_DATE=AS_OF_DATE, CLIENT_ID=CLIENT_ID, conn=conn)

    docs: List[Document] = []
    metadatas: List[Dict[str, Any]] = []
    ids: List[str] = []

        # --- build documents ---
    for text, meta in traverse_json(response_json):
        uid = str(uuid.uuid4())
        doc = Document(page_content=text, metadata={
            "property_id": PROPERTY_ID,
            "property_code": PROPERTY_CODE,
            "as_of_date": AS_OF_DATE,
            "client_id": CLIENT_ID,
            **meta
        })
        docs.append(doc)
        metadatas.append(doc.metadata)
        ids.append(uid)

    return docs, metadatas, ids

def docs_annual_summary(
    PROPERTY_ID: str,
    PROPERTY_CODE: str,
    AS_OF_DATE: str,
    CLIENT_ID: str,
    conn,
):
    """
    Build LangChain Documents for the Annual Summary collection.
    Ensures correct year/month parsing and embeds all key metrics.
    """
    df, errs = get_annual_summary(PROPERTY_ID, PROPERTY_CODE, AS_OF_DATE, CLIENT_ID, conn)
    if errs or df is None or df.empty:
        return [], [], []

    import pandas as pd
    from datetime import datetime, timezone
    from utils.month_normalizer import normalize_month_str, month_num_for_sort

    df = df.copy()

    # --- Normalize year and month ---
    if "year" not in df.columns:
        # derive from date column if needed
        if "dates" in df.columns:
            df["year"] = pd.to_datetime(df["dates"]).dt.year
        elif "Dates" in df.columns:
            df["year"] = pd.to_datetime(df["Dates"]).dt.year
        else:
            df["year"] = pd.Timestamp(AS_OF_DATE).year
    df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    if "month" not in df.columns:
        # derive textual month if absent
        if "dates" in df.columns:
            df["month"] = pd.to_datetime(df["dates"]).dt.strftime("%B")
        elif "Dates" in df.columns:
            df["month"] = pd.to_datetime(df["Dates"]).dt.strftime("%B")

    df["month_norm"] = df["month"].apply(normalize_month_str)
    df["month_sort"] = df["month_norm"].apply(month_num_for_sort)

    # --- Sanitize numeric columns ---
    num_cols = [
        "current_occ","current_rms","current_adr","current_rev",
        "stly_occ","stly_rms","stly_adr","stly_rev",
        "total_ly_occ","total_ly_rms","total_ly_adr","total_ly_rev",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    df = df.sort_values(by=["year", "month_sort"], na_position="last")

    def fmt(x, pct=False):
        if pd.isna(x):
            return "data unavailable"
        try:
            return f"{float(x):.2f}%" if pct else f"{float(x):.2f}"
        except Exception:
            return str(x)

    from langchain_core.documents import Document
    import re
    docs, metadatas, ids = [], [], []
    seen_ids = set()
    as_of_ts = int(datetime.strptime(AS_OF_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    for row in df.itertuples(index=False):
        y = int(getattr(row, "year", pd.Timestamp(AS_OF_DATE).year))
        m_str = str(getattr(row, "month_norm", "Unknown")).capitalize()
        month_sort_val = int(getattr(row, "month_sort", 13) or 13)

        # --- Rich text for retrieval ---
        text = (
            f"Annual Summary for {m_str} {y} (Property {PROPERTY_CODE}) as of {AS_OF_DATE}.\n"
            f"- Occupancy: {fmt(getattr(row, 'current_occ', None), pct=True)}\n"
            f"- Rooms Sold: {fmt(getattr(row, 'current_rms', None))}\n"
            f"- ADR: {fmt(getattr(row, 'current_adr', None))}\n"
            f"- Total Revenue: {fmt(getattr(row, 'current_rev', None))}\n"
            f"- RevPAR: {fmt((getattr(row, 'current_occ', 0) or 0) * (getattr(row, 'current_adr', 0) or 0) / 100)}\n"
            f"- STLY Occupancy: {fmt(getattr(row, 'stly_occ', None), pct=True)}\n"
            f"- STLY ADR: {fmt(getattr(row, 'stly_adr', None))}\n"
            f"- STLY Revenue: {fmt(getattr(row, 'stly_rev', None))}\n"
            f"- LY Final Occupancy: {fmt(getattr(row, 'total_ly_occ', None), pct=True)}\n"
            f"- LY Final ADR: {fmt(getattr(row, 'total_ly_adr', None))}\n"
            f"- LY Final Revenue: {fmt(getattr(row, 'total_ly_rev', None))}\n"
        )

        meta = {
            "type": "annual_summary",
            "property_id": PROPERTY_ID,
            "property_code": PROPERTY_CODE,
            "client_id": CLIENT_ID,
            "as_of_date": AS_OF_DATE,
            "as_of_date_timestamp": as_of_ts,
            "year": y,
            "month": m_str,
            "month_sort": month_sort_val,
            # key metrics for precise filters
            "current_occ": float(getattr(row, "current_occ", 0) or 0),
            "current_adr": float(getattr(row, "current_adr", 0) or 0),
            "current_rev": float(getattr(row, "current_rev", 0) or 0),
            "stly_occ": float(getattr(row, "stly_occ", 0) or 0),
            "stly_adr": float(getattr(row, "stly_adr", 0) or 0),
            "stly_rev": float(getattr(row, "stly_rev", 0) or 0),
        }

        base_id = f"annual_summary_{PROPERTY_CODE}_{AS_OF_DATE}_{y}_{m_str}"
        uid = base_id
        n = 2
        while uid in seen_ids:
            uid = f"{base_id}_{n}"
            n += 1
        seen_ids.add(uid)

        docs.append(Document(page_content=text, metadata=meta))
        metadatas.append(meta)
        ids.append(uid)

    return docs, metadatas, ids

def traverse_json(obj, parent_key=""):
    """
    Recursively walk nested JSON and yield (text, metadata) pairs.
    """
    if isinstance(obj, dict):
        for k, v in obj.items():
            new_key = f"{parent_key}.{k}" if parent_key else k
            yield from traverse_json(v, new_key)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            new_key = f"{parent_key}[{i}]"
            yield from traverse_json(item, new_key)
    else:
        # leaf node → convert into text + metadata
        yield str(obj), {"path": parent_key}


