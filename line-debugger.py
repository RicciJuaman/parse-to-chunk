import json
from azure.storage.blob import BlobServiceClient
import re
import os
import dotenv

dotenv.load_dotenv()

STORAGE_CONN_STRING = os.getenv("STORAGE_CONN_STRING")
PARSED_CONTAINER = "parsed"
BLOB_NAME = "pdf/Criminal Code Act 1899.json"

blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
container = blob_service.get_container_client(PARSED_CONTAINER)

blob = container.get_blob_client(BLOB_NAME)
doc = json.loads(blob.download_blob().readall())

print("\n🔍 SHOWING RAW LINES (FIRST 5 PAGES)\n")

for page in doc["pages"][:5]:
    print("=" * 90)
    print(f"PAGE {page['page_number']}")
    print("=" * 90)

    # HARD split only — no sentence heuristics
    lines = page["text"].splitlines()

    for i, line in enumerate(lines[:30], 1):
        print(f"[{i:02}] {line}")
