import re
import json
from typing import List, Dict, Optional
from azure.storage.blob import BlobServiceClient
import os
import dotenv

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
# STRUCTURE PATTERNS
# -------------------------
class StructureDetector:
    """Detect legal document structure using regex patterns."""
    
    # Chapter pattern: "Chapter 33 Offences against liberty"
    CHAPTER_PATTERN = re.compile(r'^Chapter\s+(\d+[A-Z]?)\s+(.+?)$', re.IGNORECASE)
    
    # Section pattern: "354 Kidnapping" or "354A Kidnapping for ransom"
    SECTION_PATTERN = re.compile(r'^(\d+[A-Z]?)\s+([A-Z].+?)(?:\s+\d+)?$')
    
    # Subsection pattern: "(1)", "(2)", "(a)", "(b)"
    SUBSECTION_PATTERN = re.compile(r'^\(([0-9a-z]+)\)')
    
    # Part pattern: "Part 6 Offences relating to property"
    PART_PATTERN = re.compile(r'^Part\s+(\d+[A-Z]?)\s+(.+?)$', re.IGNORECASE)
    
    # Division pattern: "Division 1 Stealing and like offences"
    DIVISION_PATTERN = re.compile(r'^Division\s+(\d+)\s+(.+?)$', re.IGNORECASE)
    
    @classmethod
    def detect_structure_type(cls, text: str) -> tuple[str, Optional[Dict]]:
        """
        Detect what type of structure this text represents.
        
        Returns:
            (structure_type, metadata_dict)
        """
        text = text.strip()
        
        # Check for chapter
        match = cls.CHAPTER_PATTERN.match(text)
        if match:
            return ("chapter", {
                "number": match.group(1),
                "title": match.group(2).strip()
            })
        
        # Check for part
        match = cls.PART_PATTERN.match(text)
        if match:
            return ("part", {
                "number": match.group(1),
                "title": match.group(2).strip()
            })
        
        # Check for division
        match = cls.DIVISION_PATTERN.match(text)
        if match:
            return ("division", {
                "number": match.group(1),
                "title": match.group(2).strip()
            })
        
        # Check for section
        match = cls.SECTION_PATTERN.match(text)
        if match:
            return ("section", {
                "number": match.group(1),
                "title": match.group(2).strip()
            })
        
        # Check for subsection
        match = cls.SUBSECTION_PATTERN.match(text)
        if match:
            return ("subsection", {
                "number": match.group(1)
            })
        
        # Default: regular text
        return ("text", None)


# -------------------------
# CHUNKER
# -------------------------
class LegalDocumentChunker:
    """Create chunks from parsed legal documents."""
    
    def __init__(self):
        self.detector = StructureDetector()
    
    def chunk_document(self, parsed_doc: dict) -> List[Dict]:
        """
        Convert parsed document into addressable chunks.
        
        Strategy: Chunk by SECTION (one chunk per section).
        """
        chunks = []
        
        # Current context (for breadcrumbs)
        current_chapter = None
        current_part = None
        current_division = None
        current_section = None
        
        # Accumulate text for current section
        section_text_buffer = []
        
        for page_data in parsed_doc["pages"]:
            page_num = page_data["page_number"]
            page_text = page_data["text"]
            
            # Split into sentences/lines for finer detection
            lines = self._split_into_lines(page_text)
            
            for line in lines:
                structure_type, metadata = self.detector.detect_structure_type(line)
                
                if structure_type == "chapter":
                    # Save previous section if exists
                    if current_section:
                        chunks.append(self._create_chunk(
                            current_section,
                            section_text_buffer,
                            current_chapter,
                            current_part,
                            current_division
                        ))
                        section_text_buffer = []
                    
                    # Update context
                    current_chapter = metadata
                    current_part = None
                    current_division = None
                    current_section = None
                
                elif structure_type == "part":
                    # Save previous section
                    if current_section:
                        chunks.append(self._create_chunk(
                            current_section,
                            section_text_buffer,
                            current_chapter,
                            current_part,
                            current_division
                        ))
                        section_text_buffer = []
                    
                    current_part = metadata
                    current_division = None
                    current_section = None
                
                elif structure_type == "division":
                    if current_section:
                        chunks.append(self._create_chunk(
                            current_section,
                            section_text_buffer,
                            current_chapter,
                            current_part,
                            current_division
                        ))
                        section_text_buffer = []
                    
                    current_division = metadata
                    current_section = None
                
                elif structure_type == "section":
                    # Save previous section
                    if current_section:
                        chunks.append(self._create_chunk(
                            current_section,
                            section_text_buffer,
                            current_chapter,
                            current_part,
                            current_division
                        ))
                        section_text_buffer = []
                    
                    # Start new section
                    current_section = {
                        **metadata,
                        "page": page_num
                    }
                
                else:
                    # Regular text - add to current section buffer
                    if current_section:
                        section_text_buffer.append(line)
        
        # Don't forget the last section
        if current_section and section_text_buffer:
            chunks.append(self._create_chunk(
                current_section,
                section_text_buffer,
                current_chapter,
                current_part,
                current_division
            ))
        
        return chunks
    
    def _split_into_lines(self, text: str) -> List[str]:
        """Split text into meaningful lines."""
        # Split by common sentence boundaries
        lines = re.split(r'(?<=[.!?])\s+|\n', text)
        return [line.strip() for line in lines if line.strip()]
    
    def _create_chunk(
        self,
        section: dict,
        text_buffer: List[str],
        chapter: Optional[dict],
        part: Optional[dict],
        division: Optional[dict]
    ) -> dict:
        """Create a chunk from accumulated data."""
        
        # Build breadcrumb
        breadcrumb_parts = []
        if chapter:
            breadcrumb_parts.append(f"Chapter {chapter['number']}: {chapter['title']}")
        if part:
            breadcrumb_parts.append(f"Part {part['number']}: {part['title']}")
        if division:
            breadcrumb_parts.append(f"Division {division['number']}: {division['title']}")
        
        breadcrumb = " > ".join(breadcrumb_parts) if breadcrumb_parts else ""
        
        # Full text
        full_text = " ".join(text_buffer)
        
        # Chunk ID
        chunk_id = f"section_{section['number']}"
        
        return {
            "chunk_id": chunk_id,
            "section_number": section["number"],
            "section_title": section["title"],
            "breadcrumb": breadcrumb,
            "text": full_text,
            "metadata": {
                "page": section.get("page"),
                "chapter": chapter["number"] if chapter else None,
                "part": part["number"] if part else None,
                "division": division["number"] if division else None,
                "document_type": "legislation",
                "jurisdiction": "Queensland"  # Update based on document
            }
        }


# -------------------------
# PIPELINE
# -------------------------
def run_chunking():
    """Process all parsed documents and create chunks."""
    
    chunker = LegalDocumentChunker()
    
    for blob in parsed_container.list_blobs():
        if not blob.name.endswith(".json"):
            continue
        
        print(f"Chunking: {blob.name}")
        
        chunk_name = blob.name  # Keep same name in chunks container
        
        # Skip if already chunked
        if chunks_container.get_blob_client(chunk_name).exists():
            print("  → already chunked, skipping")
            continue
        
        # Download parsed JSON
        parsed_blob = parsed_container.get_blob_client(blob.name)
        parsed_data = json.loads(parsed_blob.download_blob().readall())
        
        # Create chunks
        chunks = chunker.chunk_document(parsed_data)
        
        # Prepare output
        chunks_doc = {
            "source_document": parsed_data.get("source_document"),
            "total_chunks": len(chunks),
            "chunked_at": "2025-01-03T00:00:00Z",  # Add timestamp
            "chunks": chunks
        }
        
        # Upload to chunks container
        chunks_container.upload_blob(
            name=chunk_name,
            data=json.dumps(chunks_doc, ensure_ascii=False, indent=2),
            overwrite=True,
            content_type="application/json"
        )
        
        print(f"  → created {len(chunks)} chunks")


if __name__ == "__main__":
    run_chunking()