import json
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pymupdf

from src.relay.config import CHUNKS_DIR, PAPERS_DIR, RETRIEVAL_SETTINGS, RetrievalSettingConfig

PAPERS_SPEC = {
    "AISNY": {
        "file": "AISNY_1706.03762v7.pdf",
        "arxiv_id": "1706.03762",
        "version": "v7",
        "title": "Attention Is All You Need",
    },
    "Moshi": {
        "file": "Moshi_2410.00037v1.pdf",
        "arxiv_id": "2410.00037",
        "version": "v1",
        "title": "Moshi: a speech-text foundation model for real-time dialogue",
    },
    "DynChunk": {
        "file": "DynChunk_2507.07955v2.pdf",
        "arxiv_id": "2507.07955",
        "version": "v2",
        "title": "Dynamic Chunking for End-to-End Hierarchical Sequence Modeling",
    },
}


@dataclass
class DocumentChunk:
    chunk_id: str
    paper: str
    arxiv_id: str
    version: str
    section: str
    page: int
    text: str
    citation_tag: str


class DocumentChunker:
    def __init__(self, papers_dir: Path = PAPERS_DIR) -> None:
        self.papers_dir = papers_dir

    def extract_pages(self, pdf_filename: str) -> List[Tuple[int, str]]:
        pdf_path = self.papers_dir / pdf_filename
        if not pdf_path.exists():
            raise FileNotFoundError(f"PDF not found: {pdf_path}")

        doc = pymupdf.open(pdf_path)
        pages = []
        for idx in range(len(doc)):
            page_num = idx + 1
            raw_text = doc[idx].get_text("text")
            cleaned = self._clean_page_text(raw_text)
            pages.append((page_num, cleaned))
        return pages

    def _clean_page_text(self, text: str) -> str:
        lines = [line.strip() for line in text.splitlines()]
        filtered = []
        for line in lines:
            if not line:
                continue
            if re.match(r"^\d+\s*$", line):
                continue
            if re.match(r"^arXiv:\d+\.\d+(v\d+)?(\s+\[.*\])?$", line):
                continue
            # Phase E1: title pages and author lists are keyword magnets — they
            # contain paper-title words plus dense email/affiliation patterns and
            # outrank genuinely relevant passages in BM25. Drop author/affiliation
            # lines; keep the abstract that shares the page.
            if re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", line) or re.match(
                r"^(Abstract|†|∗|‡)", line
            ):
                continue
            if re.match(r"^[A-Z][a-z]+ [A-Z]?[a-z]*∗?†?‡? [A-Z]", line) and "University" in line:
                continue
            filtered.append(line)
        return " ".join(filtered)

    def _detect_section(self, text: str, current_section: str) -> str:
        match = re.search(r"(?:^|\n)\s*([1-9]\d*(?:\.[1-9]\d*)*)\s+([A-Z][A-Za-z0-9\s,-]{2,40})(?:\n|$)", text)
        if match:
            sec_num = match.group(1).strip()
            return f"§{sec_num}"
        return current_section

    def create_chunks(self, paper_key: str, setting: RetrievalSettingConfig) -> List[DocumentChunk]:
        spec = PAPERS_SPEC[paper_key]
        pages = self.extract_pages(spec["file"])
        chunks: List[DocumentChunk] = []

        chunk_size_words = int(setting.chunk_size_tokens * 0.75)
        overlap_words = int(setting.chunk_overlap_tokens * 0.75)
        step_words = max(10, chunk_size_words - overlap_words)

        current_sec = "§1"
        chunk_idx = 0

        for page_num, page_text in pages:
            if not page_text:
                continue

            current_sec = self._detect_section(page_text, current_sec)
            words = page_text.split()
            if not words:
                continue

            for start in range(0, len(words), step_words):
                end = min(len(words), start + chunk_size_words)
                chunk_words = words[start:end]
                if len(chunk_words) < 20 and start > 0:
                    continue

                chunk_text = " ".join(chunk_words)
                current_sec = self._detect_section(chunk_text, current_sec)
                chunk_id = f"{paper_key}-{setting.name}-{chunk_idx:04d}"
                citation = f"[{paper_key} {current_sec}, p.{page_num}]"

                chunks.append(
                    DocumentChunk(
                        chunk_id=chunk_id,
                        paper=paper_key,
                        arxiv_id=spec["arxiv_id"],
                        version=spec["version"],
                        section=current_sec,
                        page=page_num,
                        text=chunk_text,
                        citation_tag=citation,
                    )
                )
                chunk_idx += 1

        return chunks

    def build_all_settings(self, output_dir: Path = CHUNKS_DIR) -> Dict[str, int]:
        counts = {}
        for setting_key, setting_cfg in RETRIEVAL_SETTINGS.items():
            if setting_key == "A-degraded":
                continue
            target_dir = output_dir / setting_key
            target_dir.mkdir(parents=True, exist_ok=True)

            all_chunks: List[DocumentChunk] = []
            for paper_key in PAPERS_SPEC:
                chunks = self.create_chunks(paper_key, setting_cfg)
                all_chunks.extend(chunks)

            chunks_file = target_dir / "chunks.json"
            meta_file = target_dir / "metadata.json"

            with open(chunks_file, "w", encoding="utf-8") as f:
                json.dump([asdict(c) for c in all_chunks], f, indent=2)

            with open(meta_file, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "setting": setting_key,
                        "chunk_size_tokens": setting_cfg.chunk_size_tokens,
                        "chunk_overlap_tokens": setting_cfg.chunk_overlap_tokens,
                        "total_chunks": len(all_chunks),
                    },
                    f,
                    indent=2,
                )

            counts[setting_key] = len(all_chunks)

        return counts


if __name__ == "__main__":
    chunker = DocumentChunker()
    result = chunker.build_all_settings()
    print("Chunk build completed:", result)
