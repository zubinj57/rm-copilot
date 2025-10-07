# debug_vector_inspect.py
from src.common import getChromaByPropertyCode

property_code = "AC32AW"
collection_name = "annual_summary"

chroma = getChromaByPropertyCode(property_code, collection_name)
print("📚 Collection Info:", chroma._collection.count(), "documents")

res = chroma._collection.get(
    limit=5,
    include=["documents", "metadatas"]
)

for i, (doc, meta) in enumerate(zip(res["documents"], res["metadatas"]), start=1):
    print(f"\n--- Doc {i} ---")
    print("Metadata:", meta)
    print("Content:", doc[:500])
