import os
from dotenv import load_dotenv

load_dotenv()

print("MASTER_DB_HOST:", os.getenv("MASTER_DB_HOST"))
print("MASTER_DB_USERNAME:", os.getenv("MASTER_DB_USERNAME"))
print("MASTER_DB_PASSWORD:", os.getenv("MASTER_DB_PASSWORD"))
print("MASTER_DB_NAME:", os.getenv("MASTER_DB_NAME"))
print("MASTER_DB_PORT:", os.getenv("MASTER_DB_PORT"))
