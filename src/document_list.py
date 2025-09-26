from typing import List, Tuple, Dict, Any
from langchain.schema import Document
from .data_layer import fetch_daily_summaries, fetch_reservation, get_PerformanceMonitor, get_AnnualSummary
import uuid
import pandas as pd
import calendar
import re


# -----------------------------------------------------------------------------
# Document Preparation
# -----------------------------------------------------------------------------


# helper: normalize month strings and provide a numeric key for sorting (if needed)
_MONTH_ALIASES = {
    "jan":"January","january":"January",
    "feb":"February","february":"February",
    "mar":"March","march":"March",
    "apr":"April","april":"April",
    "may":"May",
    "jun":"June","june":"June",
    "jul":"July","july":"July",
    "aug":"August","august":"August",
    "sep":"September","sept":"September","september":"September",
    "oct":"October","october":"October",
    "nov":"November","november":"November",
    "dec":"December","december":"December",
}
_MONTH_TO_NUM = {m:i for i,m in enumerate(
    ["January","February","March","April","May","June","July","August","September","October","November","December"],
    start=1
)}

def normalize_month_str(m) -> str:
    if m is None or (isinstance(m, float) and pd.isna(m)):
        return "Unknown"
    s = str(m).strip()
    key = re.sub(r"[^A-Za-z]", "", s).lower()  # keep letters only
    return _MONTH_ALIASES.get(key, s.title())  # best-effort: title-case fallback

def month_num_for_sort(m_str: str) -> int | None:
    m_norm = normalize_month_str(m_str)
    return _MONTH_TO_NUM.get(m_norm)  # None if Unknown/invalid

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
 
def performance_monitor_docs(PROPERTY_ID="", PROPERTY_CODE="", AS_OF_DATE="", CLIENT_ID="",conn= None) -> Tuple[List[Document], List[Dict[str, Any]], List[str]]:
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

def annual_summary_docs(
    PROPERTY_ID: str,
    PROPERTY_CODE: str,
    AS_OF_DATE: str,
    CLIENT_ID: str,
    conn
):
    df, errs = get_AnnualSummary(PROPERTY_ID, PROPERTY_CODE, AS_OF_DATE, CLIENT_ID, conn)
    if errs or df is None or df.empty:
        return [], [], []

    # DO NOT coerce month to int; keep it as string
    # Optional: sort nicely by year then calendar month
    df = df.copy()
    if "year" in df:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")
    df["month_norm"] = df["month"].apply(normalize_month_str)
    df["month_sort"] = df["month_norm"].apply(month_num_for_sort)
    df = df.sort_values(by=["year", "month_sort", "month_norm"], na_position="last")

    docs, metadatas, ids = [], [], []
    seen_ids = set()

    for _, row in df.iterrows():
        y = int(row["year"]) if pd.notna(row["year"]) else None
        m_str = normalize_month_str(row.get("month"))
        # Build readable text
        text = (
            f"Annual Summary Snapshot AsOfDate: {AS_OF_DATE} (Year: {y}, Month: {m_str})\n\n"
            f" Current (On-the-Books):\n"
            f"- Occupancy %: {row.get('current_occ')}\n"
            f"- Rooms Sold: {row.get('current_rms')}\n"
            f"- ADR: {row.get('current_adr')}\n"
            f"- Total Revenue: {row.get('current_rev')}\n\n"
            f" Same-Time Last Year (STLY):\n"
            f"- Occupancy %: {row.get('stly_occ')}\n"
            f"- Rooms Sold: {row.get('stly_rms')}\n"
            f"- ADR: {row.get('stly_adr')}\n"
            f"- Total Revenue: {row.get('stly_rev')}\n\n"
            f" Last Year (Final, Calendar-aligned):\n"
            f"- Occupancy %: {row.get('total_ly_occ')}\n"
            f"- Rooms Sold: {row.get('total_ly_rms')}\n"
            f"- ADR: {row.get('total_ly_adr')}\n"
            f"- Total Revenue: {row.get('total_ly_rev')}\n"
        )

        meta = {
            "type": "annual_summary",
            "property_id": PROPERTY_ID,
            "property_code": PROPERTY_CODE,
            "as_of_date": AS_OF_DATE,
            "client_id": CLIENT_ID,
            "year": y,
            "month": m_str,          # ← keep string in metadata
            "month_sort": row.get("month_sort"),  # optional helper for filters

            "current_occ": row.get("current_occ"),
            "current_rms": row.get("current_rms"),
            "current_adr": row.get("current_adr"),
            "current_rev": row.get("current_rev"),
            "stly_occ": row.get("stly_occ"),
            "stly_rms": row.get("stly_rms"),
            "stly_adr": row.get("stly_adr"),
            "stly_rev": row.get("stly_rev"),
            "total_ly_occ": row.get("total_ly_occ"),
            "total_ly_rms": row.get("total_ly_rms"),
            "total_ly_adr": row.get("total_ly_adr"),
            "total_ly_rev": row.get("total_ly_rev"),
        }

        # ID uses month string; make it filesystem/db friendly
        month_slug = re.sub(r"[^A-Za-z0-9]+", "-", m_str).strip("-") if m_str else "Unknown"
        base_id = f"annual_summary_{PROPERTY_CODE}_{AS_OF_DATE}_{y}-{month_slug}"  # e.g., ..._2025-September

        uid = base_id
        n = 2
        while uid in seen_ids:
            uid = f"{base_id}-{n}"
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
        