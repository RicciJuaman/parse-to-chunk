import re
import json
from azure.storage.blob import BlobServiceClient
import os
import dotenv

dotenv.load_dotenv()

STORAGE_CONN_STRING = os.getenv("STORAGE_CONN_STRING")
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
parsed_container = blob_service.get_container_client("parsed")

# Test patterns on actual data
blob = parsed_container.get_blob_client("pdf/Constitution.json")
doc = json.loads(blob.download_blob().readall())

print("Testing Constitution pattern on page 10:")
print("=" * 80)

page_text = doc["pages"][9]["text"]  # Page 10
print(page_text[:500])
print("\n" + "=" * 80)

# Test pattern
pattern = re.compile(r'^(\d+)\.\s{1,3}([A-Z][A-Za-z\s]{3,80})\.$', re.MULTILINE)
matches = pattern.findall(page_text)

print(f"\nMatches found: {len(matches)}")
for num, title in matches[:5]:
    print(f"  {num}. {title}")