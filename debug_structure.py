import re
import json
from azure.storage.blob import BlobServiceClient
import os
import dotenv

dotenv.load_dotenv()

STORAGE_CONN_STRING = os.getenv("STORAGE_CONN_STRING")
blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
parsed_container = blob_service.get_container_client("parsed")
chunks_container = blob_service.get_container_client("chunks")

# ================================================================
# DOCUMENT CONFIGS
# ================================================================

CONFIGS = {
    "constitution": {
        "section": r'^(\d+)\.\s{1,3}([A-Z][A-Za-z\s]{3,80})\.$',
        "chapter": r'Chapter\s+([IVXLC]+)[:\s.-]+([A-Z][^.\n]{10,80}?)\.',
        "part": r'Part\s+([IVXLC]+)[:\s.-]+([A-Z][^.\n]{10,80}?)\.',
    },
    "criminal_code_1899": {
        "section": r'^(\d+[A-Z]?)\s+([A-Z][a-z]{3,}(?:\s+[a-z]+){0,15})(?=\s+[A-Z(])',
        "chapter": r'Chapter\s+(\d+[A-Z]?)[:\s.-]+([A-Z][a-zA-Z\s]{10,80}?)(?=\s+\d{1,4}|\.|$)',
        "part": r'Part\s+(\d+[A-Z]?)[:\s.-]+([A-Z][a-zA-Z\s]{10,80}?)(?=\s+\d{1,4}|\.|$)',
        "division": r'Division\s+(\d+)[:\s.-]+([A-Z][a-zA-Z\s]{10,80}?)(?=\s+\d{1,4}|\.|$)',
    },
    "criminal_code_1995": {
        "section": r'^(\d+\.\d+)\s+([A-Z][a-z]{3,}(?:\s+[a-z]+){0,15})',
        "chapter": r'Chapter\s+(\d+)[:\s.-]+([A-Z][^.\n]{10,150})',
        "part": r'Part\s+(\d+\.\d+)[:\s.-]+([A-Z][^.\n]{10,150})',
    }
}

def detect_doc_type(filename):
    """Detect config from filename."""
    fn = filename.lower()
    if "constitution" in fn:
        return "constitution"
    elif "1899" in fn:
        return "criminal_code_1899"
    elif "1995" in fn:
        return "criminal_code_1995"
    return None

def is_toc_page(text):
    """Skip TOC pages (8+ lines ending with page numbers)."""
    toc_lines = re.findall(r'^.{20,}\s+\d{1,3}$', text, re.MULTILINE)
    return len(toc_lines) >= 8 or bool(re.search(r'compilation date|registered:', text, re.I))

# ================================================================
# CHUNKER
# ================================================================

def chunk_document(parsed_doc, config):
    """Chunk using config patterns."""
    patterns = {k: re.compile(v, re.MULTILINE | re.IGNORECASE) for k, v in config.items()}
    
    chunks = []
    context = {"chapter": None, "part": None, "division": None}
    current_section = None
    buffer = []
    
    for page in parsed_doc["pages"]:
        page_text = page["text"]
        
        if is_toc_page(page_text):
            continue
        
        # Split into lines - handle both newlines and sentence splits
        if '\n' in page_text:
            lines = [l.strip() for l in page_text.split('\n') if l.strip()]
        else:
            # No newlines - split on sentence boundaries
            lines = [l.strip() for l in re.split(r'(?<=[.?!])\s+(?=[A-Z0-9(])', page_text) if l.strip()]
        
        for line in lines:
            # Detect structure
            struct_type = None
            meta = None
            
            for stype in ["chapter", "part", "division", "section"]:
                if stype not in patterns:
                    continue
                
                m = patterns[stype].match(line) if stype == "section" else patterns[stype].search(line)
                if m:
                    title = m.group(2).strip() if len(m.groups()) > 1 else ""
                    title = re.sub(r'\s+\d{1,4}\s*

def make_chunk(section, buffer, context):
    """Create chunk with breadcrumb."""
    breadcrumb = []
    for k in ["chapter", "part", "division"]:
        if context.get(k):
            breadcrumb.append(f"{k.title()} {context[k]['number']}: {context[k]['title']}")
    
    return {
        "chunk_id": f"section_{section['number']}",
        "section_number": section["number"],
        "section_title": section["title"],
        "breadcrumb": " > ".join(breadcrumb),
        "text": " ".join(buffer),
        "metadata": {
            "page": section["page"],
            **{k: context[k]["number"] if context.get(k) else None for k in ["chapter", "part", "division"]}
        }
    }

# ================================================================
# MAIN
# ================================================================

def run(test_mode=False):
    for blob in parsed_container.list_blobs():
        if not blob.name.endswith(".json"):
            continue
        
        print(f"\n📄 {blob.name}")
        
        doc_type = detect_doc_type(blob.name)
        if not doc_type:
            print("   ❌ Unknown type")
            continue
        
        print(f"   📋 Type: {doc_type}")
        
        config = CONFIGS[doc_type]
        parsed = json.loads(parsed_container.get_blob_client(blob.name).download_blob().readall())
        
        # Debug: count pages
        total_pages = len(parsed["pages"])
        skipped = sum(1 for p in parsed["pages"] if is_toc_page(p["text"]))
        print(f"   📊 Pages: {total_pages} total, {skipped} skipped")
        
        chunks = chunk_document(parsed, config)
        
        print(f"   ✅ {len(chunks)} chunks")
        
        if test_mode and chunks:
            for i, c in enumerate(chunks[:3], 1):
                print(f"\n   [{i}] {c['section_number']}: {c['section_title']}")
                if c['breadcrumb']:
                    print(f"       📍 {c['breadcrumb']}")
                print(f"       📝 {c['text'][:100]}...")
        
        if not test_mode:
            chunks_container.upload_blob(
                name=blob.name,
                data=json.dumps({"source": blob.name, "total": len(chunks), "chunks": chunks}, indent=2),
                overwrite=True
            )

if __name__ == "__main__":
    import sys
    run(test_mode="--test" in sys.argv)
, '', title)  # Clean trailing numbers
                    struct_type = stype
                    meta = {"number": m.group(1), "title": title}
                    break
            
            # Handle structure
            if struct_type in ["chapter", "part", "division"]:
                if current_section:
                    chunks.append(make_chunk(current_section, buffer, context))
                    buffer = []
                    current_section = None
                context[struct_type] = meta
                if struct_type == "chapter":
                    context["part"] = context["division"] = None
                elif struct_type == "part":
                    context["division"] = None
            
            elif struct_type == "section":
                if current_section:
                    chunks.append(make_chunk(current_section, buffer, context))
                    buffer = []
                current_section = {**meta, "page": page["page_number"]}
                buffer.append(line)
            
            elif current_section:
                buffer.append(line)
    
    if current_section:
        chunks.append(make_chunk(current_section, buffer, context))
    
    return chunks

def make_chunk(section, buffer, context):
    """Create chunk with breadcrumb."""
    breadcrumb = []
    for k in ["chapter", "part", "division"]:
        if context.get(k):
            breadcrumb.append(f"{k.title()} {context[k]['number']}: {context[k]['title']}")
    
    return {
        "chunk_id": f"section_{section['number']}",
        "section_number": section["number"],
        "section_title": section["title"],
        "breadcrumb": " > ".join(breadcrumb),
        "text": " ".join(buffer),
        "metadata": {
            "page": section["page"],
            **{k: context[k]["number"] if context.get(k) else None for k in ["chapter", "part", "division"]}
        }
    }

# ================================================================
# MAIN
# ================================================================

def run(test_mode=False):
    for blob in parsed_container.list_blobs():
        if not blob.name.endswith(".json"):
            continue
        
        print(f"\n📄 {blob.name}")
        
        doc_type = detect_doc_type(blob.name)
        if not doc_type:
            print("   ❌ Unknown type")
            continue
        
        config = CONFIGS[doc_type]
        parsed = json.loads(parsed_container.get_blob_client(blob.name).download_blob().readall())
        chunks = chunk_document(parsed, config)
        
        print(f"   ✅ {len(chunks)} chunks")
        
        if test_mode and chunks:
            for i, c in enumerate(chunks[:3], 1):
                print(f"\n   [{i}] {c['section_number']}: {c['section_title']}")
                if c['breadcrumb']:
                    print(f"       📍 {c['breadcrumb']}")
                print(f"       📝 {c['text'][:100]}...")
        
        if not test_mode:
            chunks_container.upload_blob(
                name=blob.name,
                data=json.dumps({"source": blob.name, "total": len(chunks), "chunks": chunks}, indent=2),
                overwrite=True
            )

if __name__ == "__main__":
    import sys
    run(test_mode="--test" in sys.argv)