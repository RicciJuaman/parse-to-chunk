import re
import json
from azure.storage.blob import BlobServiceClient

# -------------------------
# CONFIG
# -------------------------
STORAGE_CONN_STRING = "DefaultEndpointsProtocol=https;AccountName=cognilaw;AccountKey=9DAFdP2hUiMN1AAtPlnoM9lg1bOkQzlwhIj4heWvmU+S83uEiYUpyMPmStCJ9o3LOT4JWINuYiMl+AStm85MfQ==;EndpointSuffix=core.windows.net"
PARSED_CONTAINER = "parsed"
BLOB_NAME = "pdf/Criminal Code Act 1899.json"

# -------------------------
# STRICT SECTION HEADER REGEX
# Matches:
#   "1 Short title"
#   "2 Establishment of Code—schedule 1"
# Does NOT match:
#   "1 January 2026"
#   "2026 Act No. 5"
# -------------------------
SECTION_HEADER = re.compile(
    r'^\s*(\d+[A-Z]?)\s+([A-Z][A-Za-z0-9 ,—\-()]{3,120})\s*$'
)

SUBSECTION = re.compile(r'^\(\d+\)|^\([a-z]\)')

MONTHS = {
    "January","February","March","April","May","June",
    "July","August","September","October","November","December"
}

# -------------------------
# HELPERS
# -------------------------
def split_lines(text: str):
    if "\n" in text:
        return [l.strip() for l in text.splitlines() if l.strip()]
    # fallback
    return re.split(r'(?<=[.!?])\s+(?=[A-Z(])', text)

# -------------------------
# LOAD DOCUMENT
# -------------------------
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
parsed_container = blob_service.get_container_client(PARSED_CONTAINER)

blob = parsed_container.get_blob_client(BLOB_NAME)
parsed = json.loads(blob.download_blob().readall())

# -------------------------
# BUILD LINEAR TEXT + PAGE OFFSETS
# -------------------------
full_text = ""
page_offsets = []

for page in parsed["pages"]:
    start = len(full_text)
    full_text += page["text"] + "\n"
    end = len(full_text)

    page_offsets.append({
        "page": page["page_number"],
        "start": start,
        "end": end
    })

# -------------------------
# FIND SECTION HEADERS WITH OFFSETS
# -------------------------
sections = []

cursor = 0
for page in parsed["pages"]:
    page_num = page["page_number"]
    lines = split_lines(page["text"])

    for line in lines:
        m = SECTION_HEADER.match(line)
        if not m:
            continue

        number, title = m.group(1), m.group(2)
        first_word = title.split()[0]

        # reject months and TOC artifacts
        if first_word in MONTHS:
            continue
        if re.search(r'\s\d{1,4}$', line):  # TOC row
            continue

        offset = full_text.find(line, cursor)
        if offset == -1:
            continue

        sections.append({
            "number": number,
            "title": title,
            "start_offset": offset,
            "start_page": page_num
        })

        cursor = offset + len(line)

# -------------------------
# COMPUTE SECTION RANGES
# -------------------------
for i, sec in enumerate(sections):
    start = sec["start_offset"]
    end = sections[i+1]["start_offset"] if i+1 < len(sections) else len(full_text)

    sec["end_offset"] = end

    # page range
    start_page = sec["start_page"]
    end_page = start_page
    for p in page_offsets:
        if p["start"] <= end <= p["end"]:
            end_page = p["page"]
            break

    sec["end_page"] = end_page

    # full text
    text = full_text[start:end].strip()
    sec["text"] = text

    # subsection count
    sec["subsections"] = len([
        line for line in split_lines(text)
        if SUBSECTION.match(line)
    ])

# -------------------------
# PRINT FULL DEBUG (FIRST 5 SECTIONS)
# -------------------------
print(f"\n📘 Total sections detected: {len(sections)}\n")

for i, sec in enumerate(sections[:5], 1):
    print("=" * 100)
    print(f"SECTION {sec['number']} — {sec['title']}")
    print(f"Pages: {sec['start_page']} → {sec['end_page']}")
    print(f"Subsections: {sec['subsections']}")
    print("-" * 100)
    print(sec["text"])   # FULL TEXT — NO TRUNCATION
    print()

print("\n✅ Full-section debug complete")
