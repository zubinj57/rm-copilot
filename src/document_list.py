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

def docs_annual_summary(
    PROPERTY_ID: str,
    PROPERTY_CODE: str,
    AS_OF_DATE: str,
    CLIENT_ID: str,
    conn
):
    df, errs = get_annual_summary(PROPERTY_ID, PROPERTY_CODE, AS_OF_DATE, CLIENT_ID, conn)
    if errs or df is None or df.empty:
        return [], [], []

    df = df.copy()

    # Ensure numeric year if present
    if "year" in df.columns:
        df["year"] = pd.to_numeric(df["year"], errors="coerce").astype("Int64")

    # Normalize month & sorting key (uses your month_normalizer module)
    df["month_norm"] = df["month"].apply(normalize_month_str)
    df["month_sort"] = df["month_norm"].apply(month_num_for_sort)

    # Optional: sanitize numeric columns (robust to dirty inputs)
    num_cols = [
        "current_occ","current_rms","current_adr","current_rev",
        "stly_occ","stly_rms","stly_adr","stly_rev",
        "total_ly_occ","total_ly_rms","total_ly_adr","total_ly_rev",
    ]
    for c in num_cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # Sort: year -> month number -> month name (for ties), Unknown last
    df = df.sort_values(by=["year", "month_sort", "month_norm"], na_position="last")

    def fmt(x, pct=False):
        if pd.isna(x): return "NA"
        try:
            return f"{float(x):.2f}%" if pct else f"{float(x):.2f}"
        except Exception:
            return str(x)

    docs, metadatas, ids = [], [], []
    seen_ids = set()
    as_of_ts = int(datetime.strptime(AS_OF_DATE, "%Y-%m-%d").replace(tzinfo=timezone.utc).timestamp())

    # Faster loop for large dataframes
    for row in df.itertuples(index=False):
        # Access via attribute names
        y = int(row.year) if getattr(row, "year", None) is not None and pd.notna(row.year) else None
        m_str = getattr(row, "month_norm", "Unknown")
        # Always capitalize month for consistency (e.g., "September")
        if isinstance(m_str, str):
            m_str = m_str.capitalize()

        text = (
            f"Annual Summary Snapshot AsOfDate: {AS_OF_DATE} (Year: {y}, Month: {m_str})\n\n"
            f"- Current Occupancy %: {fmt(getattr(row, 'current_occ', None), pct=True)}\n"
            f"- Current Rooms Sold: {fmt(getattr(row, 'current_rms', None))}\n"
            f"- Current ADR: {fmt(getattr(row, 'current_adr', None))}\n"
            f"- Current Total Revenue: {fmt(getattr(row, 'current_rev', None))}\n\n"
            f"- Same Time Last Year (STLY) Occupancy %: {fmt(getattr(row, 'stly_occ', None), pct=True)}\n"
            f"- Same Time Last Year (STLY) Rooms Sold: {fmt(getattr(row, 'stly_rms', None))}\n"
            f"- Same Time Last Year (STLY) ADR: {fmt(getattr(row, 'stly_adr', None))}\n"
            f"- Same Time Last Year (STLY) Total Revenue: {fmt(getattr(row, 'stly_rev', None))}\n\n"
            f"- Last Year Final Occupancy %: {fmt(getattr(row, 'total_ly_occ', None), pct=True)}\n"
            f"- Last Year Final Rooms Sold: {fmt(getattr(row, 'total_ly_rms', None))}\n"
            f"- Last Year Final ADR: {fmt(getattr(row, 'total_ly_adr', None))}\n"
            f"- Last Year Final Total Revenue: {fmt(getattr(row, 'total_ly_rev', None))}\n"
        )

        # Build metadata safely
        month_sort_val = getattr(row, "month_sort", None)
        month_sort_val = int(month_sort_val) if month_sort_val is not None and pd.notna(month_sort_val) else None

        meta = {
            "type": "annual_summary",
            "property_id": PROPERTY_ID,
            "property_code": PROPERTY_CODE,
            "as_of_date": AS_OF_DATE,
            "as_of_date_timestamp": as_of_ts,
            "client_id": CLIENT_ID,
            "year": y,
            "month": m_str,
            "month_sort": getattr(row, "month_sort", None),

            # 🔹 Core KPIs
            "current_occ": getattr(row, "current_occ", None),
            "current_rms": getattr(row, "current_rms", None),
            "current_adr": getattr(row, "current_adr", None),
            "current_rev": getattr(row, "current_rev", None),
            "stly_occ": getattr(row, "stly_occ", None),
            "stly_rms": getattr(row, "stly_rms", None),
            "stly_adr": getattr(row, "stly_adr", None),
            "stly_rev": getattr(row, "stly_rev", None),
            "total_ly_occ": getattr(row, "total_ly_occ", None),
            "total_ly_rms": getattr(row, "total_ly_rms", None),
            "total_ly_adr": getattr(row, "total_ly_adr", None),
            "total_ly_rev": getattr(row, "total_ly_rev", None),

            # 🔹 Aliases
            "otb": getattr(row, "current_rms", None),        # OTB = rooms sold
            "rooms_sold": getattr(row, "current_rms", None),
            "occupancy": getattr(row, "current_occ", None),
            "occ": getattr(row, "current_occ", None),
            "adr": getattr(row, "current_adr", None),
            "rate": getattr(row, "current_adr", None),
            "revenue": getattr(row, "current_rev", None),
            "total_revenue": getattr(row, "current_rev", None),
        }

        # Stable, readable ID
        y_str = str(y) if y is not None else "UnknownYear"
        month_slug = re.sub(r"[^A-Za-z0-9]+", "-", m_str).strip("-") if m_str else "Unknown"
        base_id = f"annual_summary_{PROPERTY_CODE}_{AS_OF_DATE}_{y_str}-{month_slug}"

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


