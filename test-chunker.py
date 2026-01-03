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

blob_service = BlobServiceClient.from_connection_string(STORAGE_CONN_STRING)
parsed_container = blob_service.get_container_client(PARSED_CONTAINER)

# -------------------------
# STRUCTURE DETECTOR
# -------------------------
class StructureDetector:
    """
    Detects structural elements in Australian legislation.
    Designed for INLINE headings (real-world PDFs).
    """

    CHAPTER = re.compile(r'\bChapter\s+([IVXLC\d]+)\.?\s*(.+)?', re.IGNORECASE)
    PART = re.compile(r'\bPart\s+([IVXLC\d]+)\.?\s*(.+)?', re.IGNORECASE)
    DIVISION = re.compile(r'\bDivision\s+(\d+)\.?\s*(.+)?', re.IGNORECASE)

    # INLINE SECTION: "7. The Senate shall be composed..."
    SECTION_HEADER = re.compile(
    r'^\s*(\d+[A-Z]?)\s+([A-Z][A-Za-z ,\-()]{3,100})'
    )

    SUBSECTION = re.compile(r'^\(([0-9a-z]+)\)')

    @classmethod
    def detect(cls, text: str):
        text = text.strip()
        if not text:
            return "text", None

        if m := cls.CHAPTER.search(text):
            return "chapter", {
                "number": m.group(1),
                "title": (m.group(2) or "").strip()
            }

        if m := cls.PART.search(text):
            return "part", {
                "number": m.group(1),
                "title": (m.group(2) or "").strip()
            }

        if m := cls.DIVISION.search(text):
            return "division", {
                "number": m.group(1),
                "title": (m.group(2) or "").strip()
            }

        if m := cls.SECTION_HEADER.match(text):
            return "section", {
                "number": m.group(1),
                "title": m.group(2).strip()
            }

        if m := cls.SUBSECTION.match(text):
            return "subsection", {"number": m.group(1)}

        return "text", None


# -------------------------
# CHUNKER
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
            lines = self._split(page["text"])

            for line in lines:
                kind, meta = self.detector.detect(line)

                if kind in {"chapter", "part", "division"}:
                    if current_section:
                        chunks.append(
                            self._flush(current_section, buffer,
                                        current_chapter, current_part, current_division)
                        )
                        buffer = []
                        current_section = None

                    if kind == "chapter":
                        current_chapter = meta
                        current_part = None
                        current_division = None
                    elif kind == "part":
                        current_part = meta
                        current_division = None
                    elif kind == "division":
                        current_division = meta

                elif kind == "section":
                    if current_section:
                        chunks.append(
                            self._flush(current_section, buffer,
                                        current_chapter, current_part, current_division)
                        )
                        buffer = []

                    current_section = {
                        **meta,
                        "page_start": page_num
                    }
                    buffer.append(line)

                else:
                    if current_section:
                        buffer.append(line)

        if current_section and buffer:
            chunks.append(
                self._flush(current_section, buffer,
                            current_chapter, current_part, current_division)
            )

        return chunks

    def _split(self, text: str) -> List[str]:
        return [
            s.strip() for s in
            re.split(r'(?<=[.;])\s+(?=[A-Z(])|\n+', text)
            if s.strip()
        ]

    def _flush(
        self,
        section: dict,
        buffer: List[str],
        chapter: Optional[dict],
        part: Optional[dict],
        division: Optional[dict],
    ) -> dict:

        breadcrumb = []
        if chapter:
            breadcrumb.append(f"Chapter {chapter['number']}")
        if part:
            breadcrumb.append(f"Part {part['number']}")
        if division:
            breadcrumb.append(f"Division {division['number']}")

        return {
            "chunk_id": f"section_{section['number']}",
            "section_number": section["number"],
            "section_title": section["title"],
            "breadcrumb": " > ".join(breadcrumb),
            "text": " ".join(buffer),
            "metadata": {
                "page_start": section["page_start"],
                "jurisdiction": "Australia",
                "document_type": "legislation"
            }
        }


# -------------------------
# TEST
# -------------------------
def test(blob_name: str):
    blob = parsed_container.get_blob_client(blob_name)
    parsed = json.loads(blob.download_blob().readall())

    chunker = LegalChunker()
    chunks = chunker.chunk(parsed)

    print(f"\n✅ Created {len(chunks)} chunks\n")

    for c in chunks[:3]:
        print("=" * 60)
        print(f"{c['chunk_id']}")
        print(f"{c['section_number']} – {c['section_title']}")
        print(f"{c['breadcrumb']}")
        print(c["text"][:300], "...\n")


if __name__ == "__main__":
    test("pdf/Criminal Code Act 1899.json")
