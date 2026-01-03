import re
import json
from typing import List, Dict, Optional
from azure.storage.blob import BlobServiceClient
import os
import dotenv

dotenv.load_dotenv()

# -------------------------
# CONFIG
# -------------------------
STORAGE_CONN_STRING = os.getenv("STORAGE_CONN_STRING")
PARSED_CONTAINER = "parsed"
CHUNKS_CONTAINER = "chunks"

blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
parsed_container = blob_service.get_container_client(PARSED_CONTAINER)
chunks_container = blob_service.get_container_client(CHUNKS_CONTAINER)

# -------------------------
# STRUCTURE DETECTOR
# -------------------------
class StructureDetector:
    # More flexible patterns
    CHAPTER = re.compile(
        r'(?:^|\s)Chapter\s+([IVXLC\d]+[A-Z]?)[\s.:—-]*(.+)?',
        re.IGNORECASE | re.MULTILINE
    )
    PART = re.compile(
        r'(?:^|\s)Part\s+([IVXLC\d]+[A-Z]?)[\s.:—-]*(.+)?',
        re.IGNORECASE | re.MULTILINE
    )
    DIVISION = re.compile(
        r'(?:^|\s)Division\s+(\d+[A-Z]?)[\s.:—-]*(.+)?',
        re.IGNORECASE | re.MULTILINE
    )
    
    # Section: "354 Kidnapping" or "354A Kidnapping for ransom"
    # Must start at beginning of line/string
    SECTION_HEADER = re.compile(
        r'^(\d+[A-Z]?)\s+([A-Z][A-Za-z\s,\-()]{3,120})(?:\s|$)',
        re.MULTILINE
    )
    
    SUBSECTION = re.compile(r'^\s*\(([0-9a-z]+)\)', re.MULTILINE)

    @classmethod
    def detect(cls, text: str):
        text = text.strip()
        if not text:
            return "text", None

        # Check in order of specificity
        if m := cls.CHAPTER.search(text):
            return "chapter", {
                "number": m.group(1).strip(),
                "title": (m.group(2) or "").strip()
            }

        if m := cls.PART.search(text):
            return "part", {
                "number": m.group(1).strip(),
                "title": (m.group(2) or "").strip()
            }

        if m := cls.DIVISION.search(text):
            return "division", {
                "number": m.group(1).strip(),
                "title": (m.group(2) or "").strip()
            }

        if m := cls.SECTION_HEADER.match(text):
            return "section", {
                "number": m.group(1).strip(),
                "title": m.group(2).strip()
            }

        if m := cls.SUBSECTION.match(text):
            return "subsection", {"number": m.group(1)}

        return "text", None


# -------------------------
# IMPROVED CHUNKER
# -------------------------
class LegalChunker:
    def __init__(self):
        self.detector = StructureDetector()

    def chunk(self, parsed_doc: dict) -> List[Dict]:
        chunks = []

        current_chapter = None
        current_part = None
        current_division = None
        current_section = None
        buffer = []

        for page in parsed_doc["pages"]:
            page_num = page["page_number"]
            lines = self._split_text(page["text"])

            for line in lines:
                kind, meta = self.detector.detect(line)

                if kind == "chapter":
                    # Flush previous section
                    if current_section:
                        chunks.append(
                            self._create_chunk(
                                current_section, buffer,
                                current_chapter, current_part, current_division
                            )
                        )
                        buffer = []
                        current_section = None
                    
                    current_chapter = meta
                    current_part = None
                    current_division = None

                elif kind == "part":
                    if current_section:
                        chunks.append(
                            self._create_chunk(
                                current_section, buffer,
                                current_chapter, current_part, current_division
                            )
                        )
                        buffer = []
                        current_section = None
                    
                    current_part = meta
                    current_division = None

                elif kind == "division":
                    if current_section:
                        chunks.append(
                            self._create_chunk(
                                current_section, buffer,
                                current_chapter, current_part, current_division
                            )
                        )
                        buffer = []
                        current_section = None
                    
                    current_division = meta

                elif kind == "section":
                    # Flush previous section
                    if current_section:
                        chunks.append(
                            self._create_chunk(
                                current_section, buffer,
                                current_chapter, current_part, current_division
                            )
                        )
                        buffer = []

                    # Start new section
                    current_section = {
                        **meta,
                        "page_start": page_num
                    }
                    buffer.append(line)

                else:
                    # Regular text or subsection
                    if current_section:
                        buffer.append(line)

        # Flush final section
        if current_section and buffer:
            chunks.append(
                self._create_chunk(
                    current_section, buffer,
                    current_chapter, current_part, current_division
                )
            )

        return chunks

    def _split_text(self, text: str) -> List[str]:
        """
        Split text into processable chunks.
        Try multiple strategies.
        """
        # Strategy 1: If text has newlines, use them
        if '\n' in text:
            lines = [line.strip() for line in text.split('\n') if line.strip()]
            if lines:
                return lines
        
        # Strategy 2: Split on sentence boundaries before capitals or parens
        lines = re.split(r'(?<=[.;:])\s+(?=[A-Z(0-9])', text)
        return [line.strip() for line in lines if line.strip()]

    def _create_chunk(
        self,
        section: dict,
        buffer: List[str],
        chapter: Optional[dict],
        part: Optional[dict],
        division: Optional[dict],
    ) -> dict:
        """Create chunk with metadata."""
        
        breadcrumb = []
        if chapter and chapter.get('number'):
            title = f"Chapter {chapter['number']}"
            if chapter.get('title'):
                title += f": {chapter['title']}"
            breadcrumb.append(title)
            
        if part and part.get('number'):
            title = f"Part {part['number']}"
            if part.get('title'):
                title += f": {part['title']}"
            breadcrumb.append(title)
            
        if division and division.get('number'):
            title = f"Division {division['number']}"
            if division.get('title'):
                title += f": {division['title']}"
            breadcrumb.append(title)

        return {
            "chunk_id": f"section_{section['number']}",
            "section_number": section["number"],
            "section_title": section["title"],
            "breadcrumb": " > ".join(breadcrumb) if breadcrumb else "",
            "text": " ".join(buffer),
            "metadata": {
                "page_start": section.get("page_start"),
                "chapter": chapter.get("number") if chapter else None,
                "part": part.get("number") if part else None,
                "division": division.get("number") if division else None,
                "jurisdiction": "Queensland",
                "document_type": "legislation"
            }
        }


# -------------------------
# PIPELINE
# -------------------------
def run_chunking(test_mode=False):
    """Process parsed documents and create chunks."""
    
    chunker = LegalChunker()
    
    for blob in parsed_container.list_blobs():
        if not blob.name.endswith(".json"):
            continue
        
        print(f"\n📄 Processing: {blob.name}")
        
        # Download parsed JSON
        parsed_blob = parsed_container.get_blob_client(blob.name)
        parsed_data = json.loads(parsed_blob.download_blob().readall())
        
        # Create chunks
        chunks = chunker.chunk(parsed_data)
        
        print(f"   ✅ Created {len(chunks)} chunks")
        
        if test_mode and chunks:
            print("\n   First 3 chunks:")
            for i, chunk in enumerate(chunks[:3], 1):
                print(f"\n   [{i}] Section {chunk['section_number']}: {chunk['section_title']}")
                print(f"       Breadcrumb: {chunk['breadcrumb']}")
                print(f"       Text preview: {chunk['text'][:100]}...")
        
        if not test_mode:
            # Save to chunks container
            chunk_name = blob.name
            chunks_doc = {
                "source_document": parsed_data.get("source_document"),
                "total_chunks": len(chunks),
                "chunks": chunks
            }
            
            chunks_container.upload_blob(
                name=chunk_name,
                data=json.dumps(chunks_doc, ensure_ascii=False, indent=2),
                overwrite=True,
                content_type="application/json"
            )
            print(f"   💾 Saved to {CHUNKS_CONTAINER}/{chunk_name}")


if __name__ == "__main__":
    import sys
    
    # Run in test mode if --test flag provided
    test_mode = "--test" in sys.argv
    
    if test_mode:
        print("🧪 RUNNING IN TEST MODE (no saves)\n")
    
    run_chunking(test_mode=test_mode)