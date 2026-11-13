import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

from rank_bm25 import BM25Okapi

from src.relay.config import CHUNKS_DIR, RETRIEVAL_SETTINGS, RetrievalSettingConfig


@dataclass
class RetrievedPassage:
    chunk_id: str
    paper: str
    arxiv_id: str
    version: str
    section: str
    page: int
    text: str
    citation_tag: str
    score: float


class RetrievalEngine:
    def __init__(self, chunks_base_dir: Path = CHUNKS_DIR) -> None:
        self.chunks_base_dir = chunks_base_dir
        self.indices: Dict[str, BM25Okapi] = {}
        self.chunk_stores: Dict[str, List[Dict]] = {}
        self._reranker = None
        self._load_indices()

    def _tokenize(self, text: str) -> List[str]:
        return re.findall(r"\b\w+\b", text.lower())

    def _load_indices(self) -> None:
        for setting_key in ["A", "B", "C"]:
            target_key = "A" if setting_key == "C" else setting_key
            chunks_path = self.chunks_base_dir / target_key / "chunks.json"
            if not chunks_path.exists():
                continue

            with open(chunks_path, "r", encoding="utf-8") as f:
                chunks = json.load(f)

            self.chunk_stores[setting_key] = chunks
            tokenized_corpus = [self._tokenize(c["text"]) for c in chunks]
            self.indices[setting_key] = BM25Okapi(tokenized_corpus)

    def _get_reranker(self):
        if self._reranker is None:
            try:
                from sentence_transformers import CrossEncoder

                self._reranker = CrossEncoder("cross-encoder/ms-marco-MiniLM-L-6-v2")
            except Exception:
                self._reranker = False
        return self._reranker

    def search(
        self,
        query: str,
        target_paper: Optional[str] = None,
        setting_name: str = "A",
    ) -> List[RetrievedPassage]:
        setting_cfg: RetrievalSettingConfig = RETRIEVAL_SETTINGS.get(
            setting_name, RETRIEVAL_SETTINGS["A"]
        )
        index_key = "A" if setting_name in ("A", "C", "A-degraded") else "B"

        bm25 = self.indices.get(index_key)
        chunks = self.chunk_stores.get(index_key, [])
        if not bm25 or not chunks:
            return []

        tokenized_query = self._tokenize(query)
        if not tokenized_query:
            return []

        raw_scores = bm25.get_scores(tokenized_query)

        candidates = []
        for idx, score in enumerate(raw_scores):
            chunk = chunks[idx]
            paper = chunk["paper"]

            adjusted_score = float(score)
            if target_paper and target_paper.upper() != "ALL":
                if paper.upper() == target_paper.upper():
                    adjusted_score *= 1.3
                else:
                    adjusted_score *= 0.5

            candidates.append((adjusted_score, chunk))

        candidates.sort(key=lambda x: x[0], reverse=True)
        top_candidates = candidates[: setting_cfg.top_k]

        results = [
            RetrievedPassage(
                chunk_id=c["chunk_id"],
                paper=c["paper"],
                arxiv_id=c["arxiv_id"],
                version=c["version"],
                section=c["section"],
                page=c["page"],
                text=c["text"],
                citation_tag=c["citation_tag"],
                score=score,
            )
            for score, c in top_candidates
        ]

        if setting_cfg.rerank and results:
            reranker = self._get_reranker()
            if reranker:
                pairs = [[query, r.text] for r in results]
                rerank_scores = reranker.predict(pairs)
                for i, r in enumerate(results):
                    r.score = float(rerank_scores[i])
                results.sort(key=lambda x: x.score, reverse=True)
            results = results[: setting_cfg.rerank_top_n]

        return results
