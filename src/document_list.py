from typing import List, Tuple, Dict, Any
from langchain.schema import Document
from .data_layer import fetch_daily_summaries, fetch_reservation, get_PerformanceMonitor, get_AnnualSummary
import uuid

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

    response_json, error_list = get_PerformanceMonitor(PROPERTY_ID=PROPERTY_ID, PROPERTY_CODE=PROPERTY_CODE, AS_OF_DATE=AS_OF_DATE, CLIENT_ID=CLIENT_ID, conn=conn, year=year)

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
    PROPERTY_ID: str = "",
    PROPERTY_CODE: str = "",
    AS_OF_DATE: str = "",
    CLIENT_ID: str = "",
    year: str = "",
    conn=None
    
) -> Tuple[List[Document], List[Dict[str, Any]], List[str]]:
    """Convert annual summary data into Document objects, metadata, and IDs."""
    print('run annual_summary_docs')
    response_json, error_list = get_AnnualSummary(
        PROPERTY_ID=PROPERTY_ID,
        PROPERTY_CODE=PROPERTY_CODE,
        AS_OF_DATE=AS_OF_DATE,
        CLIENT_ID=CLIENT_ID,
        year=year,
        conn=conn,
        componentname="AnnualSummary"
    )

    docs: List[Document] = []
    metadatas: List[Dict[str, Any]] = []
    ids: List[str] = []

    if not response_json:
        return docs, metadatas, ids

    # Flatten nested JSON using the same traverse_json helper
    for text, meta in traverse_json(response_json):
        uid = str(uuid.uuid4())
        doc = Document(
            page_content=text,
            metadata={
                "property_id": PROPERTY_ID,
                "property_code": PROPERTY_CODE,
                "as_of_date": AS_OF_DATE,
                "client_id": CLIENT_ID,
                "year": year,
                **meta
            },
        )
        docs.append(doc)
        metadatas.append(doc.metadata)
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