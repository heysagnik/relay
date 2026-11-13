import asyncio
import json
import math
import random
import re
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

try:
    from relay.config import DATA_DIR, MODEL_DRAFT, MODEL_TRIAGE
except ImportError:
    from src.relay.config import DATA_DIR, MODEL_DRAFT, MODEL_TRIAGE

app = FastAPI(title="Relay Stand-in Model Service", version="1.0.0")

# Calibrated physics simulator configured for HALF daily quota
DEFAULT_CONFIG = {
    "error_rate": 0.03,
    "slow_success_rate": 0.04,
    "slow_multiplier_min": 5.0,
    "slow_multiplier_max": 12.0,
    "degrade_ms_per_inflight": 40.0,
    "soft_concurrency_knee": 3,
    "models": {
        MODEL_TRIAGE: {
            "rpm_limit": 30,
            "tpm_limit": 8000,
            "rpd_limit": 500,    # Half of measured real RPD (1,000)
            "tpd_limit": 100000, # Half of documented real TPD (200,000, provider docs)
            "ttft_mu": 4.55,     # Calibrated: real TTFT median ~93 ms on 27B => ln(93)=4.53 (calibration.json)
            "ttft_sigma": 0.30,  # Real spread p5..p95: 92..132 ms plus rare tail outliers
            "itt_ms": 14.0,
        },
        MODEL_DRAFT: {
            "rpm_limit": 30,
            "tpm_limit": 8000,
            "rpd_limit": 500,    # Half of measured real RPD (1,000)
            "tpd_limit": 100000, # Half of documented real TPD (200,000, provider docs)
            "ttft_mu": 5.89,     # Calibrated: real TTFT median ~363 ms on 120B => ln(363)=5.89 (calibration.json)
            "ttft_sigma": 0.25,  # Real spread p5..p95: 200..412 ms
            "itt_ms": 30.0,
        },
    },
}

STATE = {
    "config": DEFAULT_CONFIG,
    "inflight_requests": 0,
    "total_requests": 0,
    "total_tokens_out": 0,
    "rate_limited_count": 0,
    "models": {
        MODEL_TRIAGE: {
            "requests_minute": [],
            "tokens_minute": [],
            "requests_day": 0,
            "tokens_day": 0,
        },
        MODEL_DRAFT: {
            "requests_minute": [],
            "tokens_minute": [],
            "requests_day": 0,
            "tokens_day": 0,
        },
    },
}


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatCompletionRequest(BaseModel):
    model: str
    messages: List[ChatMessage]
    temperature: Optional[float] = 0.0
    max_tokens: Optional[int] = 1024
    stream: Optional[bool] = False
    stream_options: Optional[Dict[str, Any]] = None


STANDIN_STATE_FILE = DATA_DIR / "standin_state.json"


def _save_state() -> None:
    """Persist quota windows so a restart does not silently reset the day."""
    try:
        STANDIN_STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "date": datetime.utcnow().strftime("%Y-%m-%d"),
            "models": {
                m: {
                    "requests_day": s["requests_day"],
                    "tokens_day": s["tokens_day"],
                }
                for m, s in STATE["models"].items()
            },
        }
        STANDIN_STATE_FILE.write_text(json.dumps(payload))
    except Exception:
        pass


def _load_state() -> None:
    try:
        if not STANDIN_STATE_FILE.exists():
            return
        payload = json.loads(STANDIN_STATE_FILE.read_text())
        today = datetime.utcnow().strftime("%Y-%m-%d")
        if payload.get("date") != today:
            return
        for m, s in payload.get("models", {}).items():
            if m in STATE["models"]:
                STATE["models"][m]["requests_day"] = int(s.get("requests_day", 0))
                STATE["models"][m]["tokens_day"] = int(s.get("tokens_day", 0))
    except Exception:
        pass


_load_state()


def prune_standin_windows(model_key: str, now: float) -> None:
    m_state = STATE["models"].setdefault(
        model_key,
        {"requests_minute": [], "tokens_minute": [], "requests_day": 0, "tokens_day": 0},
    )
    cutoff = now - 60.0
    m_state["requests_minute"] = [t for t in m_state["requests_minute"] if t > cutoff]
    m_state["tokens_minute"] = [[t, tok] for t, tok in m_state["tokens_minute"] if t > cutoff]


def extract_context_passages(messages: List[ChatMessage]) -> List[Tuple[str, str]]:
    passages = []
    full_text = "\n".join(m.content for m in messages)
    pattern = r"Passage \(([^\)]+)\):\s*\n(.*?)(?=\nPassage \(|\n\nQuestion:|$)"
    matches = re.findall(pattern, full_text, flags=re.DOTALL)
    for tag, body in matches:
        clean_body = body.strip()
        if clean_body:
            passages.append((tag.strip(), clean_body))
    return passages


def generate_dynamic_triage(question: str) -> str:
    q_lower = question.lower()

    out_of_corpus_entities = [
        "gpt-4", "gpt4", "whisper", "dollars", "cost in us dollars",
        "learning rate schedule was used to pre-train",
    ]
    if any(k in q_lower for k in out_of_corpus_entities):
        return json.dumps(
            {
                "classification": "out_of_scope",
                "target_paper": "None",
                "queries": [question],
            }
        )

    if "how many attention heads" in q_lower and not any(
        p in q_lower for p in ["attention is all you need", "transformer", "moshi", "dynchunk"]
    ):
        return json.dumps(
            {
                "classification": "ambiguous",
                "target_paper": "ALL",
                "queries": [question],
            }
        )

    target = "ALL"
    if any(k in q_lower for k in ["attention is all you need", "transformer", "bleu", "d_model", "positional"]):
        target = "AISNY"
    elif any(k in q_lower for k in ["moshi", "mimi", "audio", "inner monologue", "codebook"]):
        target = "Moshi"
    elif any(k in q_lower for k in ["chunk", "hierarchical", "segment"]):
        target = "DynChunk"

    return json.dumps(
        {
            "classification": "answerable",
            "target_paper": target,
            "queries": [question],
        }
    )


_STOPWORDS = {
    "the", "a", "an", "is", "are", "was", "were", "what", "how", "does", "do",
    "did", "of", "in", "for", "to", "and", "or", "on", "at", "by", "with",
    "from", "that", "this", "it", "its", "as", "be", "used", "use", "which",
    "between", "compare", "not", "can", "model", "models",
}


def _content_words(text: str) -> set:
    return {w for w in re.findall(r"[a-z0-9]+", text.lower()) if w not in _STOPWORDS and len(w) > 2}


def generate_dynamic_answer(question: str, passages: List[Tuple[str, str]]) -> str:
    """Synthesize a grounded, cited answer from the retrieved passages.

    Calibrated behavior: pick the sentences with the highest lexical overlap
    with the question (the way an extractive grounded model would), and refuse
    when no retrieved sentence is relevant. This is context-derived, never
    canned text, and honest about gaps.
    """
    if not passages:
        return "Refusal: The research library does not contain sufficient evidence to answer this question."

    q_words = _content_words(question)
    best: List[Tuple[float, str, str]] = []  # (score, sentence, tag)
    for tag, p_text in passages:
        sentences = re.split(r"(?<=[.!?])\s+", p_text)
        for s in sentences:
            s_clean = s.strip()
            if len(s_clean.split()) < 6:
                continue
            s_words = _content_words(s_clean)
            if not s_words:
                continue
            overlap = len(q_words & s_words)
            jaccard = overlap / max(1, len(q_words | s_words))
            score = overlap * 2.0 + jaccard
            best.append((score, s_clean, tag))

    if not best or best[0][0] <= 0.0:
        return (
            "Refusal: The retrieved passages do not state the answer to this question, "
            "so I cannot cite a source for it."
        )

    best.sort(key=lambda x: x[0], reverse=True)
    selected: List[str] = []
    used_tags: set = set()
    for score, sentence, tag in best:
        if len(selected) >= 2:
            break
        if tag in used_tags and len(selected) >= 1:
            continue
        clause = sentence if sentence.endswith((".", "!", "?")) else sentence + "."
        selected.append(f"{clause} {tag}")
        used_tags.add(tag)

    return " ".join(selected)


@app.get("/__config")
def get_config():
    return STATE["config"]


@app.post("/__config")
def update_config(new_config: Dict[str, Any]):
    STATE["config"].update(new_config)
    return STATE["config"]


@app.post("/__state")
def seed_state(new_state: Dict[str, Any]):
    """Test hook: pre-seed daily counters (e.g. '3pm, budget nearly gone')."""
    for m, s in new_state.get("models", {}).items():
        if m in STATE["models"]:
            if "requests_day" in s:
                STATE["models"][m]["requests_day"] = int(s["requests_day"])
            if "tokens_day" in s:
                STATE["models"][m]["tokens_day"] = int(s["tokens_day"])
    _save_state()
    return {
        m: {"requests_day": s["requests_day"], "tokens_day": s["tokens_day"]}
        for m, s in STATE["models"].items()
    }


@app.get("/__stats")
def get_stats():
    return {
        "inflight_requests": STATE["inflight_requests"],
        "total_requests": STATE["total_requests"],
        "rate_limited_count": STATE["rate_limited_count"],
        "models": {
            m: {
                "rpm_current": len(STATE["models"][m]["requests_minute"]),
                "tpm_current": sum(tok for _, tok in STATE["models"][m]["tokens_minute"]),
                "rpd_current": STATE["models"][m]["requests_day"],
                "tpd_current": STATE["models"][m]["tokens_day"],
                "rpd_limit": STATE["config"]["models"][m]["rpd_limit"],
                "tpd_limit": STATE["config"]["models"][m]["tpd_limit"],
            }
            for m in STATE["models"]
        },
    }


@app.post("/v1/chat/completions")
async def chat_completions(req: ChatCompletionRequest):
    now = time.time()
    model = req.model
    cfg = STATE["config"]
    m_cfg = cfg["models"].get(model, cfg["models"][MODEL_TRIAGE])

    prune_standin_windows(model, now)
    m_state = STATE["models"][model]

    current_rpm = len(m_state["requests_minute"])
    current_tpm = sum(t for _, t in m_state["tokens_minute"])
    rpm_limit = m_cfg["rpm_limit"]
    tpm_limit = m_cfg["tpm_limit"]
    rpd_limit = m_cfg["rpd_limit"]
    tpd_limit = m_cfg["tpd_limit"]

    rem_rpm = max(0, rpm_limit - current_rpm)
    rem_tpm = max(0, tpm_limit - current_tpm)
    rem_rpd = max(0, rpd_limit - m_state["requests_day"])

    headers = {
        "x-ratelimit-limit-requests": str(rpd_limit),
        "x-ratelimit-limit-tokens": str(tpm_limit),
        "x-ratelimit-remaining-requests": str(rem_rpd),
        "x-ratelimit-remaining-tokens": str(rem_tpm),
        "x-ratelimit-reset-requests": "12h4m",
        "x-ratelimit-reset-tokens": "2.1s",
    }

    # Name the exact window hit in the body, mirroring the real provider's
    # 429 format so clients can classify minute-window (retryable) vs
    # day-window (never retry) exhaustion.
    hit_minute = current_rpm >= rpm_limit or current_tpm >= tpm_limit
    hit_daily = (
        m_state["requests_day"] >= rpd_limit
        or m_state["tokens_day"] >= tpd_limit
    )

    if hit_minute or hit_daily:
        STATE["rate_limited_count"] += 1
        if hit_daily:
            reason = (
                f"on tokens per day (TPD): Limit {tpd_limit:,}, Used {m_state['tokens_day']:,}. "
                if m_state["tokens_day"] >= tpd_limit
                else f"on requests per day (RPD): Limit {rpd_limit:,}, Used {m_state['requests_day']:,}. "
            )
            scope = "day"
            headers["retry-after"] = "3600"
        else:
            reason = (
                f"on tokens per minute (TPM): Limit {tpm_limit:,}, Used {current_tpm:,}. "
                if current_tpm >= tpm_limit
                else f"on requests per minute (RPM): Limit {rpm_limit}, Used {current_rpm}. "
            )
            scope = "minute"
            headers["retry-after"] = "2.5"
        error_payload = {
            "error": {
                "message": (
                    f"Rate limit reached for model `{model}` in organization `org-standin` "
                    f"service tier `on_demand` {reason}"
                    + ("Please try again in 2.5s." if scope == "minute" else "Quota resets at 00:00 UTC.")
                ),
                "type": "tokens" if "tokens" in reason else "requests",
                "code": "rate_limit_exceeded",
            }
        }
        return JSONResponse(status_code=429, content=error_payload, headers=headers)

    if random.random() < cfg["error_rate"]:
        return JSONResponse(
            status_code=500,
            content={"error": {"message": "Internal server error in stand-in simulation"}},
            headers=headers,
        )

    STATE["inflight_requests"] += 1
    STATE["total_requests"] += 1

    full_prompt = " ".join(m.content for m in req.messages)
    tokens_in = max(10, int(len(full_prompt) / 3.8))

    is_triage = any("JSON" in m.content for m in req.messages) or "triage" in full_prompt.lower()
    if is_triage:
        question = req.messages[-1].content
        text_out = generate_dynamic_triage(question)
    else:
        passages = extract_context_passages(req.messages)
        question = req.messages[-1].content
        text_out = generate_dynamic_answer(question, passages)

    tokens_out = max(15, int(len(text_out) / 3.8))

    mu = m_cfg.get("ttft_mu", 5.5)
    sigma = m_cfg.get("ttft_sigma", 0.35)
    raw_ttft_ms = math.exp(random.gauss(mu, sigma))

    inflight_excess = max(0, STATE["inflight_requests"] - cfg.get("soft_concurrency_knee", 3))
    degradation_ms = inflight_excess * cfg.get("degrade_ms_per_inflight", 40.0)
    ttft_ms = raw_ttft_ms + degradation_ms

    if random.random() < cfg.get("slow_success_rate", 0.04):
        mult = random.uniform(cfg["slow_multiplier_min"], cfg["slow_multiplier_max"])
        ttft_ms *= mult

    # Book usage at admission (before any TTFT sleep): concurrent arrivals
    # within the sleep window must not each see a pre-burst window and
    # collectively overshoot the limit.
    now_rec = time.time()
    m_state["requests_minute"].append(now_rec)
    m_state["tokens_minute"].append([now_rec, tokens_in + tokens_out])
    m_state["requests_day"] += 1
    m_state["tokens_day"] += tokens_in + tokens_out
    _save_state()

    async def sse_stream():
        try:
            await asyncio.sleep(ttft_ms / 1000.0)

            # Counters were booked at request acceptance (below), mirroring a
            # provider that accounts usage from the moment it admits a request.

            words = text_out.split()
            itt_s = (m_cfg.get("itt_ms", 20.0) / 1000.0) * 1.2

            for idx, word in enumerate(words):
                chunk = {
                    "id": f"chatcmpl-standin-{int(time.time()*1000)}",
                    "object": "chat.completion.chunk",
                    "created": int(time.time()),
                    "model": model,
                    "choices": [
                        {
                            "index": 0,
                            "delta": {"content": word + " "},
                            "finish_reason": None,
                        }
                    ],
                }
                yield f"data: {json.dumps(chunk)}\n\n"
                await asyncio.sleep(itt_s)

            final_chunk = {
                "id": f"chatcmpl-standin-{int(time.time()*1000)}",
                "object": "chat.completion.chunk",
                "created": int(time.time()),
                "model": model,
                "choices": [{"index": 0, "delta": {}, "finish_reason": "stop"}],
                "usage": {"prompt_tokens": tokens_in, "completion_tokens": tokens_out},
            }
            yield f"data: {json.dumps(final_chunk)}\n\n"
            yield "data: [DONE]\n\n"

        finally:
            STATE["inflight_requests"] = max(0, STATE["inflight_requests"] - 1)

    if req.stream:
        return StreamingResponse(sse_stream(), media_type="text/event-stream", headers=headers)

    await asyncio.sleep(ttft_ms / 1000.0)
    STATE["inflight_requests"] = max(0, STATE["inflight_requests"] - 1)
    # Counters were booked at admission above.

    return JSONResponse(
        content={
            "id": f"chatcmpl-standin-{int(time.time()*1000)}",
            "object": "chat.completion",
            "model": model,
            "choices": [{"index": 0, "message": {"role": "assistant", "content": text_out}}],
            "usage": {"prompt_tokens": tokens_in, "completion_tokens": tokens_out},
        },
        headers=headers,
    )
