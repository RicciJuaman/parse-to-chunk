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
    """
    Detects structure in legal documents with STRICT validation.
    """
    
    # Chapter/Part/Division patterns
    CHAPTER = re.compile(
        r'Chapter\s+([IVXLC\d]+[A-Z]?)[\s.:—–-]+([A-Z][^\n]{10,150})',
        re.IGNORECASE
    )
    PART = re.compile(
        r'Part\s+([IVXLC\d]+[A-Z]?)[\s.:—–-]+([A-Z][^\n]{10,150})',
        re.IGNORECASE
    )
    DIVISION = re.compile(
        r'Division\s+(\d+[A-Z]?)[\s.:—–-]+([A-Z][^\n]{10,150})',
        re.IGNORECASE
    )
    
    # STRICT SECTION PATTERNS by document type
    
    # Constitution style: "7.  The Senate."
    # - Number + period
    # - Two spaces (sometimes one)
    # - Title (capitalized)
    # - Ends with period
    SECTION_CONSTITUTION = re.compile(
        r'^(\d+[A-Z]?)\.\s{1,3}([A-Z][A-Za-z\s]{3,80})\.$',
        re.MULTILINE
    )
    
    # Criminal Code style: "354 Kidnapping" or "354A Kidnapping for ransom"
    # - Number (with optional letter)
    # - Single space
    # - Title (NO period at end, appears mid-flow)
    # - Must be followed by actual content (not a page number)
    SECTION_CODE = re.compile(
        r'^(\d+[A-Z]?)\s+([A-Z][a-z]{3,}(?:\s+[a-z]+){0,10})(?=\s+[A-Z(]|\s*$)',
        re.MULTILINE
    )
    
    SUBSECTION = re.compile(r'^\s*\(([0-9]+|[a-z]+)\)\s+', re.MULTILINE)

    @classmethod
    def detect(cls, text: str, doc_type: str = "auto"):
        """
        Detect structure type with document-specific patterns.
        
        doc_type: "constitution", "code", or "auto"
        """
        text = text.strip()
        if not text or len(text) < 5:
            return "text", None

        # Structural elements (same for all doc types)
        if m := cls.CHAPTER.search(text):
            title = m.group(2).strip().rstrip('.')
            if len(title) > 10:
                return "chapter", {
                    "number": m.group(1).strip(),
                    "title": title
                }

        if m := cls.PART.search(text):
            title = m.group(2).strip().rstrip('.')
            if len(title) > 10:
                return "part", {
                    "number": m.group(1).strip(),
                    "title": title
                }

        if m := cls.DIVISION.search(text):
            title = m.group(2).strip().rstrip('.')
            if len(title) > 10:
                return "division", {
                    "number": m.group(1).strip(),
                    "title": title
                }

        # Section detection - try both patterns
        section_match = None
        
        # Try Constitution pattern first (more strict)
        if m := cls.SECTION_CONSTITUTION.match(text):
            section_match = (m.group(1), m.group(2).strip())
        # Try Code pattern if Constitution didn't match
        elif m := cls.SECTION_CODE.match(text):
            number = m.group(1)
            title = m.group(2).strip()
            
            # Additional validation for Code pattern
            # Reject if title is a common false positive
            reject_words = ['Page', 'January', 'February', 'March', 'April', 'May', 
                           'June', 'July', 'August', 'September', 'October', 
                           'November', 'December', 'Compilation', 'Contents', 
                           'Registered', 'Volume', 'Schedule', 'Includes']
            
            # Check if first word is a reject word OR if title is too short
            first_word = title.split()[0] if title.split() else ""
            
            if first_word not in reject_words and len(title) >= 8:
                # Check section number is reasonable
                try:
                    num_val = int(re.sub(r'[A-Z]', '', number))
                    if 1 <= num_val <= 10000:
                        # Final check: title should have at least 2 words or be a compound word
                        word_count = len(title.split())
                        if word_count >= 2 or len(title) >= 12:
                            section_match = (number, title)
                except ValueError:
                    pass
        
        if section_match:
            return "section", {
                "number": section_match[0],
                "title": section_match[1]
            }

        if cls.SUBSECTION.match(text):
            m = re.match(r'^\s*\(([0-9]+|[a-z]+)\)', text)
            if m:
                return "subsection", {"number": m.group(1)}

        return "text", None


# -------------------------
# CHUNKER
# -------------------------
class LegalChunker:
    def __init__(self, doc_type: str = "auto"):
        self.detector = StructureDetector()
        self.doc_type = doc_type

    def chunk(self, parsed_doc: dict) -> List[Dict]:
        chunks = []

        current_chapter = None
        current_part = None
        current_division = None
        current_section = None
        buffer = []
        
        prev_section_num = None
        section_count = 0

        for page in parsed_doc["pages"]:
            page_num = page["page_number"]
            
            # Skip obvious front matter (adjust based on your docs)
            if page_num <= 3:
                # Quick check: does this page have section-like content?
                test_text = page["text"][:500]
                if not re.search(r'^\d+\.\s{1,3}[A-Z][a-z]+', test_text, re.MULTILINE):
                    if not re.search(r'^\d+[A-Z]?\s+[A-Z][a-z]+\s+[A-Z(]', test_text, re.MULTILINE):
                        continue
            
            lines = self._split_text(page["text"])

            for line in lines:
                kind, meta = self.detector.detect(line, self.doc_type)

                if kind == "chapter":
                    if current_section and buffer:
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
                    if current_section and buffer:
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
                    if current_section and buffer:
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
                    # Validation: reasonable section progression
                    try:
                        current_num = int(re.sub(r'[A-Z]', '', meta['number']))
                        
                        # Skip if suspicious
                        if prev_section_num:
                            # Don't go backwards (unless resetting at new chapter)
                            if current_num < prev_section_num and (current_num > 10):
                                continue
                            # Don't jump more than 200 sections
                            if current_num > prev_section_num + 200:
                                continue
                        
                        prev_section_num = current_num
                    except ValueError:
                        pass
                    
                    # Flush previous section
                    if current_section and buffer:
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
                    section_count += 1

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
        Split text into lines/sentences.
        Preserve section headers as standalone lines.
        """
        lines = []
        
        # If text has newlines, use them
        if '\n' in text:
            raw_lines = text.split('\n')
            
            for line in raw_lines:
                line = line.strip()
                if not line:
                    continue
                
                # If line is very long and has sentences, split it
                if len(line) > 300 and '. ' in line:
                    # Split on '. ' but keep the period
                    parts = re.split(r'(\.\s+)', line)
                    current = ""
                    for i, part in enumerate(parts):
                        current += part
                        # When we hit a split point (odd indices), save and reset
                        if i % 2 == 1 and len(current.strip()) > 50:
                            lines.append(current.strip())
                            current = ""
                    if current.strip():
                        lines.append(current.strip())
                else:
                    lines.append(line)
        else:
            # No newlines - split on sentence boundaries
            parts = re.split(r'(?<=[.?!])\s+(?=[A-Z0-9(])', text)
            lines = [p.strip() for p in parts if p.strip()]
        
        return lines

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

        # Build full text
        text = " ".join(buffer)
        
        # Remove duplicate section header from start if present
        section_header_variants = [
            f"{section['number']}.  {section['title']}.",  # Constitution style
            f"{section['number']}. {section['title']}.",
            f"{section['number']} {section['title']}",      # Code style
        ]
        
        for variant in section_header_variants:
            if text.startswith(variant):
                text = text[len(variant):].strip()
                break

        return {
            "chunk_id": f"section_{section['number']}",
            "section_number": section["number"],
            "section_title": section["title"],
            "breadcrumb": " > ".join(breadcrumb) if breadcrumb else "",
            "text": text,
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
def run_chunking(test_mode=False, limit_pages=None):
    """Process parsed documents and create chunks."""
    
    for blob in parsed_container.list_blobs():
        if not blob.name.endswith(".json"):
            continue
        
        print(f"\n📄 Processing: {blob.name}")
        
        # Download parsed JSON
        parsed_blob = parsed_container.get_blob_client(blob.name)
        parsed_data = json.loads(parsed_blob.download_blob().readall())
        
        # Detect document type from filename
        doc_type = "auto"
        if "constitution" in blob.name.lower():
            doc_type = "constitution"
            print(f"   📜 Detected: Constitution document")
        elif "code" in blob.name.lower() or "act" in blob.name.lower():
            doc_type = "code"
            print(f"   ⚖️  Detected: Code/Act document")
        
        # Limit pages for testing
        if limit_pages:
            parsed_data["pages"] = parsed_data["pages"][:limit_pages]
        
        # Create chunks
        chunker = LegalChunker(doc_type=doc_type)
        chunks = chunker.chunk(parsed_data)
        
        print(f"   ✅ Created {len(chunks)} chunks")
        
        if test_mode and chunks:
            print("\n   First 5 chunks:")
            for i, chunk in enumerate(chunks[:5], 1):
                print(f"\n   [{i}] Section {chunk['section_number']}: {chunk['section_title']}")
                if chunk['breadcrumb']:
                    print(f"       📍 {chunk['breadcrumb']}")
                print(f"       📝 {chunk['text'][:120]}...")
        
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
    
    test_mode = "--test" in sys.argv
    limit_pages = None
    
    for arg in sys.argv:
        if arg.startswith("--pages="):
            limit_pages = int(arg.split("=")[1])
    
    if test_mode:
        print("🧪 RUNNING IN TEST MODE (no saves)")
        if limit_pages:
            print(f"   Limited to first {limit_pages} pages\n")
    
    run_chunking(test_mode=test_mode, limit_pages=limit_pages)