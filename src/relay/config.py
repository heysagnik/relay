import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Dict

# Ensure project root and src/ are in sys.path
BASE_DIR = Path(__file__).resolve().parent.parent.parent
SRC_DIR = BASE_DIR / "src"

for p in (str(SRC_DIR), str(BASE_DIR)):
    if p not in sys.path:
        sys.path.insert(0, p)

# Safe stdlib .env parser (never prints, logs, or leaks keys)
def _load_env_file(path: Path = BASE_DIR / ".env") -> None:
    if not path.is_file():
        return
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, v = line.split("=", 1)
                k = k.strip()
                v = v.strip().strip("'\"")
                if k and k not in os.environ:
                    os.environ[k] = v
    except Exception:
        pass

_load_env_file()

DATA_DIR = BASE_DIR / "data"
PAPERS_DIR = BASE_DIR / "papers"
CHUNKS_DIR = DATA_DIR / "chunks"
RESULTS_DIR = BASE_DIR / "results"
LOGS_DIR = BASE_DIR / "logs"

MODEL_TRIAGE = "qwen/qwen3.8-27b"
MODEL_DRAFT = "openai/gpt-oss-120b"

DEFAULT_PATIENCE_MS = 15000
DEFAULT_CONCURRENCY = 4
DEFAULT_QUEUE_MAX = 40
# Adaptive concurrency bounds (Phase B3, AIMD)
MIN_CONCURRENCY = 1
MAX_CONCURRENCY = 8
# Proactive send-lease: aim to use at most this fraction of each measured window
LEASE_HEADROOM = 0.90
RPM_PRESSURE_DELAY_S = 2.0
# If the send-lease wait estimate exceeds this, degrade retrieval instead of
# waiting (the ladder's 'what do you do with retrieval when the limit is close').
LEASE_DEGRADE_WAIT_S = 10.0
# Token estimation for the send-lease. Measured on this corpus: paper text
# tokenizes at ~4.2 chars/token (numbers/tables inflate it; prose dilutes it),
# and completions average ~300 tokens against a 600 cap.
TOKEN_CHARS_RATIO = 4.2
EST_COMPLETION_TOKENS = 300
TIMEOUT_CONNECT_S = 5.0
TIMEOUT_TTFT_S = 12.0
TIMEOUT_INTER_TOKEN_S = 10.0
TIMEOUT_TOTAL_S = 45.0

SCORE_GATE_THRESHOLD = 6.0
# Cross-encoder logits are a different scale from BM25 scores: relevant passages
# score ~1.5-6.6, irrelevant ~<1.5 and often negative (measured 2026-11-13 with
# ms-marco-MiniLM-L-6-v2 on the three-paper corpus). Use a separate, near-zero
# gate for reranked settings; only refuse when nothing scores as relevant at all.
RERANK_GATE_THRESHOLD = 0.0

PRICE_TRIAGE_INPUT_PER_1M = 0.15
PRICE_TRIAGE_OUTPUT_PER_1M = 0.20
PRICE_DRAFT_INPUT_PER_1M = 0.80
PRICE_DRAFT_OUTPUT_PER_1M = 1.20

STANDIN_HOST = "127.0.0.1"
STANDIN_PORT = 8001
STANDIN_BASE_URL = f"http://{STANDIN_HOST}:{STANDIN_PORT}/v1"
STANDIN_STATS_URL = f"http://{STANDIN_HOST}:{STANDIN_PORT}/__stats"
STANDIN_CONFIG_URL = f"http://{STANDIN_HOST}:{STANDIN_PORT}/__config"
GROQ_BASE_URL = "https://api.groq.com/openai/v1"

@dataclass(frozen=True)
class RetrievalSettingConfig:
    name: str
    chunk_size_tokens: int
    chunk_overlap_tokens: int
    top_k: int
    rerank: bool
    rerank_top_n: int

RETRIEVAL_SETTINGS: Dict[str, RetrievalSettingConfig] = {
    "A": RetrievalSettingConfig(
        name="A",
        chunk_size_tokens=350,
        chunk_overlap_tokens=50,
        top_k=6,
        rerank=False,
        rerank_top_n=6,
    ),
    "B": RetrievalSettingConfig(
        name="B",
        chunk_size_tokens=150,
        chunk_overlap_tokens=25,
        top_k=12,
        rerank=False,
        rerank_top_n=12,
    ),
    "C": RetrievalSettingConfig(
        name="C",
        chunk_size_tokens=350,
        chunk_overlap_tokens=50,
        top_k=6,
        rerank=True,
        rerank_top_n=3,
    ),
    "A-degraded": RetrievalSettingConfig(
        name="A-degraded",
        chunk_size_tokens=350,
        chunk_overlap_tokens=50,
        top_k=2,
        rerank=False,
        rerank_top_n=2,
    ),
}
