import asyncio
import json
import re
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Dict, List, Optional, Tuple

try:
    from relay.budget import BudgetLedger
    from relay.config import (
        DEFAULT_CONCURRENCY,
        DEFAULT_PATIENCE_MS,
        DEFAULT_QUEUE_MAX,
        LOGS_DIR,
        MAX_CONCURRENCY,
        MIN_CONCURRENCY,
        MODEL_DRAFT,
        MODEL_TRIAGE,
        LEASE_DEGRADE_WAIT_S,
        RETRIEVAL_SETTINGS,
        RERANK_GATE_THRESHOLD,
        SCORE_GATE_THRESHOLD,
    )
    from relay.groq_client import GroqClient, StreamChunk
    from relay.retrieval import RetrievalEngine, RetrievedPassage
except ImportError:
    from src.relay.budget import BudgetLedger
    from src.relay.config import (
        DEFAULT_CONCURRENCY,
        DEFAULT_PATIENCE_MS,
        DEFAULT_QUEUE_MAX,
        LOGS_DIR,
        MAX_CONCURRENCY,
        MIN_CONCURRENCY,
        MODEL_DRAFT,
        MODEL_TRIAGE,
        LEASE_DEGRADE_WAIT_S,
        RETRIEVAL_SETTINGS,
        RERANK_GATE_THRESHOLD,
        SCORE_GATE_THRESHOLD,
    )
    from src.relay.groq_client import GroqClient, StreamChunk
    from src.relay.retrieval import RetrievalEngine, RetrievedPassage

SPANS_LOG_FILE = LOGS_DIR / "spans.jsonl"
SPANS_ROTATE_BYTES = 5_000_000

TRIAGE_JSON_FORMAT = {"type": "json_object"}

DRAFT_SYSTEM_PROMPT = (
    "You answer questions strictly from provided research-paper passages. "
    "Every factual statement MUST end with its exact citation tag like [Paper §X, p.Y]. "
    "If the passages do not state the answer, say so plainly instead of guessing. "
    "Never invent citations."
)

TRIAGE_SYSTEM_PROMPT = (
    "You are a triage assistant for a paper QA system covering 3 pinned arXiv papers:\n"
    "1. Attention Is All You Need (AISNY)\n"
    "2. Moshi: a speech-text foundation model (Moshi)\n"
    "3. Dynamic Chunking for Sequence Modeling (DynChunk)\n\n"
    "If the question is primarily about something outside these three papers "
    "(another model such as Whisper or GPT-4, a company, a dataset owner), "
    "classify it as out_of_scope.\n"
    'Respond in JSON with keys: {"classification": "answerable|out_of_scope|ambiguous", '
    '"target_paper": "AISNY|Moshi|DynChunk|ALL", "queries": ["query1"]}'
)

PASSAGE_MAX_CHARS = 1200


class AdaptiveConcurrencyLimiter:
    """AIMD concurrency limiter with priority waiters.

    Capacity adapts to provider behaviour: additive increase on success,
    multiplicative decrease on 429s or degraded TTFT. Waiters are admitted
    highest-priority first, FIFO within a class."""

    def __init__(self, initial: int = DEFAULT_CONCURRENCY) -> None:
        self._capacity = max(MIN_CONCURRENCY, min(MAX_CONCURRENCY, initial))
        self._active = 0
        self._waiters: List[Tuple[int, int, asyncio.Future]] = []
        self._seq = 0
        self._lock = asyncio.Lock()

    @property
    def capacity(self) -> int:
        return self._capacity

    @property
    def active(self) -> int:
        return self._active

    @property
    def waiting(self) -> int:
        return len(self._waiters)

    async def acquire(self, priority: int = 5, timeout_s: Optional[float] = None) -> bool:
        """Acquire a slot, waiting at most timeout_s. Returns False on timeout."""
        async with self._lock:
            if self._active < self._capacity and not self._waiters:
                self._active += 1
                return True
            self._seq += 1
            fut: asyncio.Future = asyncio.get_event_loop().create_future()
            self._waiters.append((priority, self._seq, fut))

        try:
            if timeout_s is None:
                await fut
            else:
                await asyncio.wait_for(fut, timeout_s)
            return True
        except (asyncio.TimeoutError, asyncio.CancelledError):
            async with self._lock:
                self._waiters = [w for w in self._waiters if w[2] is not fut]
            if fut.done() and not fut.cancelled():
                self.release()
            return False

    def release(self) -> None:
        asyncio.get_event_loop().create_task(self._wake())

    async def _wake(self) -> None:
        async with self._lock:
            self._active = max(0, self._active - 1)
            if self._active < self._capacity and self._waiters:
                self._waiters.sort(key=lambda w: (w[0], w[1]))
                _, _, fut = self._waiters.pop(0)
                self._active += 1
                if not fut.done():
                    fut.set_result(None)

    def record_success(self) -> None:
        self._capacity = min(MAX_CONCURRENCY, self._capacity + 1)

    def record_rate_limited(self) -> None:
        self._capacity = max(MIN_CONCURRENCY, self._capacity // 2)

    def record_degraded(self) -> None:
        self._capacity = max(MIN_CONCURRENCY, self._capacity - 1)


@dataclass
class RequestContext:
    request_id: str
    scenario: str
    source: str
    retrieval_setting: str
    question: str
    priority: int = 5
    enqueue_time: float = field(default_factory=time.time)
    dequeue_time: Optional[float] = None
    events: List[Tuple[str, int]] = field(default_factory=list)
    retries: List[Dict] = field(default_factory=list)
    model_calls: List[Dict] = field(default_factory=list)
    queue_len_at_dequeue: int = 0
    tokens_in: int = 0
    tokens_out: int = 0
    first_token_ms: Optional[int] = None
    total_ms: int = 0
    status: str = "ok"
    answer_grade: Optional[str] = None
    final_answer: str = ""
    clauses_verified: int = 0
    loop_triggered: bool = False
    refusal_reason: Optional[str] = None
    coalesced_from: Optional[str] = None

    def record_event(self, event_name: str) -> None:
        rel_ms = int((time.time() - self.enqueue_time) * 1000)
        self.events.append((event_name, rel_ms))


def _cache_key(question: str) -> str:
    return re.sub(r"\s+", " ", question.strip().lower())


class RelayOrchestrator:
    def __init__(
        self,
        llm_client: GroqClient,
        retrieval_engine: RetrievalEngine,
        budget_ledger: BudgetLedger,
        patience_ms: int = DEFAULT_PATIENCE_MS,
        concurrency: int = DEFAULT_CONCURRENCY,
        q_max: int = DEFAULT_QUEUE_MAX,
    ) -> None:
        self.client = llm_client
        self.retrieval = retrieval_engine
        self.budget = budget_ledger
        self.patience_ms = patience_ms
        self.q_max = q_max
        self.limiter = AdaptiveConcurrencyLimiter(concurrency)
        self.spans_file = SPANS_LOG_FILE
        # Phase B5: single-flight + short-TTL cache for identical questions.
        self._inflight: Dict[str, asyncio.Future] = {}

    # ---- spans ---------------------------------------------------------

    async def _log_span(self, ctx: RequestContext) -> None:
        if getattr(ctx, "_span_logged", False):
            return  # early-return paths plus the finally block would log twice
        ctx._span_logged = True
        self.spans_file.parent.mkdir(parents=True, exist_ok=True)
        try:
            if self.spans_file.exists() and self.spans_file.stat().st_size > SPANS_ROTATE_BYTES:
                rotated = self.spans_file.with_name(
                    f"spans_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.jsonl"
                )
                self.spans_file.rename(rotated)
        except OSError:
            pass
        record = {
            "request_id": ctx.request_id,
            "scenario": ctx.scenario,
            "source": ctx.source,
            "start_time": datetime.fromtimestamp(ctx.enqueue_time, timezone.utc).isoformat(),
            "events": ctx.events,
            "retries": ctx.retries,
            "model_calls": ctx.model_calls,
            "queue_len_at_dequeue": ctx.queue_len_at_dequeue,
            "status": ctx.status,
            "refusal_reason": ctx.refusal_reason,
            "first_token_ms": ctx.first_token_ms,
            "total_ms": ctx.total_ms,
            "answer_grade": ctx.answer_grade,
            "final_answer": (ctx.final_answer or "")[:400],
            "coalesced_from": ctx.coalesced_from,
        }
        with open(self.spans_file, "a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    # ---- helpers -------------------------------------------------------

    @staticmethod
    def _make_retry_note(ctx: RequestContext) -> Callable:
        def note(stage: str, reason: str, sleep_s: float = 0.0) -> None:
            ctx.retries.append(
                {
                    "stage": stage,
                    "reason": reason,
                    "rel_ms": int((time.time() - ctx.enqueue_time) * 1000),
                    "sleep_s": round(float(sleep_s), 2),
                }
            )
            ctx.record_event(f"retry_{stage}")

        return note

    def _verify_clause_citations(
        self, clause: str, retrieved_passages: List[RetrievedPassage]
    ) -> Tuple[bool, Optional[str]]:
        # Non-capturing group so findall returns the FULL tag (e.g.
        # '[AISNY §2, p.1]'), and every citation must exactly match a tag that
        # was actually retrieved — section and page included. A fabricated
        # '[AISNY §99, p.999]' must fail even when an AISNY passage was fetched.
        citations = re.findall(r"\[(?:AISNY|Moshi|DynChunk)\s+§[0-9.]*,\s*p\.[0-9]+\]", clause)
        if not citations:
            return True, None
        allowed_tags = {p.citation_tag for p in retrieved_passages}
        for tag in citations:
            if tag not in allowed_tags:
                return False, tag
        return True, None

    @staticmethod
    def _prepare_passages(
        passages: List[RetrievedPassage], question: str
    ) -> List[RetrievedPassage]:
        """Dedupe identical texts and trim each passage to a query-centred
        window (Phase C3): head-truncation cut off relevant sentences that sat
        deep inside long chunks, so instead keep the ~1000-char window with the
        best lexical overlap with the question (head fallback when no overlap)."""
        q_words = {
            w
            for w in re.findall(r"[a-z0-9]+", question.lower())
            if len(w) > 2
        }

        def best_window(text: str) -> str:
            if len(text) <= PASSAGE_MAX_CHARS:
                return text
            window = 1000
            step = 500
            best_start, best_score = 0, -1.0
            for start in range(0, len(text) - window + 1, step):
                chunk_words = set(re.findall(r"[a-z0-9]+", text[start : start + window].lower()))
                score = len(q_words & chunk_words)
                if score > best_score:
                    best_score, best_start = score, start
            return text[best_start : best_start + window]

        seen: set = set()
        trimmed: List[RetrievedPassage] = []
        for p in passages:
            if p.text in seen:
                continue
            seen.add(p.text)
            p.text = best_window(p.text)
            trimmed.append(p)
        return trimmed

    # ---- single-flight (concurrent duplicates only) ----------------------

    async def _coalesce(self, ctx: RequestContext, key: str) -> bool:
        """If an identical question is *concurrently in flight*, wait for its
        leader and share the outcome — one provider call serves N waiters.
        Deliberately no post-completion TTL cache: replaying answers across
        requests would distort what the load scenarios measure (and the
        write-up's design stance says exactly that). Returns True when served
        from a leader."""
        leader_fut = self._inflight.get(key)
        if leader_fut is not None:
            try:
                await asyncio.wait_for(
                    asyncio.shield(leader_fut),
                    timeout=max(0.5, (self.patience_ms - (time.time() - ctx.enqueue_time) * 1000) / 1000),
                )
            except asyncio.TimeoutError:
                return False
            outcome = leader_fut.result()
            if outcome:
                self._adopt_outcome(ctx, outcome)
                return True
            return False

        self._inflight[key] = asyncio.get_event_loop().create_future()
        return False

    def _adopt_outcome(self, ctx: RequestContext, outcome: Dict) -> None:
        ctx.status = outcome["status"]
        ctx.final_answer = outcome["final_answer"]
        ctx.answer_grade = outcome["answer_grade"]
        ctx.refusal_reason = outcome["refusal_reason"]
        ctx.retrieval_setting = outcome["retrieval_setting"]
        ctx.coalesced_from = outcome["request_id"]
        ctx.first_token_ms = int((time.time() - ctx.enqueue_time) * 1000)
        ctx.total_ms = ctx.first_token_ms
        ctx.record_event("coalesced")

    def _publish(self, ctx: RequestContext, key: str) -> None:
        fut = self._inflight.pop(key, None)
        if fut is not None and not fut.done():
            if ctx.status in ("ok", "rate_limited"):
                fut.set_result({
                    "request_id": ctx.request_id,
                    "status": ctx.status,
                    "final_answer": ctx.final_answer,
                    "answer_grade": ctx.answer_grade,
                    "refusal_reason": ctx.refusal_reason,
                    "retrieval_setting": ctx.retrieval_setting,
                })
            else:
                # Leader failed (timeout/error): let followers run their own
                # request rather than propagate the failure.
                fut.set_result(None)

    # ---- main flow -------------------------------------------------------

    async def handle_request(
        self,
        request_id: str,
        question: str,
        scenario: str = "steady",
        retrieval_setting: str = "A",
        on_flush: Optional[Callable[[str], None]] = None,
        priority: int = 5,
        allow_lease_degrade: bool = True,
        use_requested_setting: bool = False,
    ) -> RequestContext:
        ctx = RequestContext(
            request_id=request_id,
            scenario=scenario,
            source=self.client.source_name,
            retrieval_setting=retrieval_setting,
            question=question,
            priority=priority,
        )
        ctx._allow_lease_degrade = allow_lease_degrade
        ctx._use_requested_setting = use_requested_setting
        ctx.record_event("enqueue")

        async def finish() -> RequestContext:
            ctx.total_ms = int((time.time() - ctx.enqueue_time) * 1000)
            await self._log_span(ctx)
            return ctx

        try:
            # Phase B5: identical in-flight or freshly-answered question?
            key = _cache_key(question)
            if await self._coalesce(ctx, key):
                if on_flush:
                    on_flush(ctx.final_answer)
                return await finish()

            # Phase B1: bound the queue so admitted requests keep their TTFT.
            if self.limiter.waiting + self.limiter.active >= self.q_max:
                ctx.status = "timeout"
                ctx.refusal_reason = "queue_full"
                ctx.final_answer = (
                    "The service is at capacity right now and the queue is full. "
                    "I'm turning this away immediately rather than making you wait "
                    "past the point where an answer is still useful. Please try again shortly."
                )
                ctx.record_event("queue_full_refusal")
                if on_flush:
                    on_flush(ctx.final_answer)
                return await finish()

            # Phase B2/B4: wait for a slot, but never past the patience budget.
            remaining_s = (self.patience_ms - (time.time() - ctx.enqueue_time) * 1000) / 1000
            if remaining_s <= 0:
                ctx.status = "timeout"
                ctx.refusal_reason = "patience_expired_in_queue"
                ctx.final_answer = (
                    "I had to give up on this request: the queue wait exceeded the patience limit."
                )
                if on_flush:
                    on_flush(ctx.final_answer)
                return await finish()

            # Budget admission BEFORE taking a concurrency slot: an RPM-pressure
            # delay must not hold a slot while sleeping (Phase 3 fix — starved
            # queued requests). Degraded-setting and model-override decisions
            # are made here and passed into the pipeline.
            admission_action, delay_s, draft_model = await self.budget.check_admission(MODEL_DRAFT)
            if delay_s:
                ctx.record_event("admission_delay")
                await asyncio.sleep(delay_s)
            if admission_action == "budget_exhausted":
                # Daily exhaustion is not a concurrency signal; do not shrink
                # the limiter for it (queued requests would die for nothing —
                # admission refuses them instantly instead).
                pass

            acquired = await self.limiter.acquire(priority=priority, timeout_s=remaining_s)
            if not acquired:
                ctx.status = "timeout"
                ctx.refusal_reason = "patience_expired_in_queue"
                ctx.final_answer = (
                    "I had to give up on this request: the queue wait exceeded the patience limit."
                )
                if on_flush:
                    on_flush(ctx.final_answer)
                return await finish()

            try:
                ctx.dequeue_time = time.time()
                ctx.queue_len_at_dequeue = self.limiter.waiting
                ctx.record_event("dequeue")
                await self._run_pipeline(
                    ctx, question, retrieval_setting, key, on_flush,
                    admission_action, draft_model,
                )
            finally:
                self.limiter.release()
        except Exception as e:
            is_rate_limit = (
                type(e).__name__ == "RateLimitExceededError" or "Rate limited (429)" in str(e)
            )
            if is_rate_limit:
                ctx.status = "rate_limited"
                ctx.answer_grade = "refused_correctly"
                ctx.refusal_reason = "provider_rate_limited_after_retries"
                ctx.final_answer = (
                    "I can't answer this one right now. Our model provider's quota is exhausted "
                    "and I retried twice before giving up rather than queue you indefinitely. "
                    "Nothing is queued behind it — please try again in a few seconds."
                )
            else:
                ctx.status = "error"
                ctx.final_answer = (
                    "I hit an internal error while answering this. Nothing is wrong with your "
                    "question — please try again."
                )
                # Detail lives only in the span log, never in the user-facing reply.
                ctx.refusal_reason = f"internal_{type(e).__name__}: {str(e)[:200]}"
            if on_flush:
                on_flush(ctx.final_answer)
        finally:
            ctx.total_ms = int((time.time() - ctx.enqueue_time) * 1000)
            await self._log_span(ctx)

        return ctx

    async def _run_pipeline(
        self,
        ctx: RequestContext,
        question: str,
        retrieval_setting: str,
        cache_key: str,
        on_flush: Optional[Callable[[str], None]],
        admission_action: str,
        draft_model: str,
    ) -> None:
        retry_note = self._make_retry_note(ctx)

        if admission_action == "budget_exhausted":
            ctx.status = "rate_limited"
            ctx.answer_grade = "refused_correctly"
            ctx.refusal_reason = "daily_quota_exhausted"
            ctx.final_answer = (
                "I can't answer this one right now. Our model provider's free-tier daily quota "
                "ran out today. Nothing is queued and retrying now would just fail. "
                "Please ask again after the quota resets at 00:00 UTC."
            )
            ctx.record_event("budget_exhausted_exit")
            if on_flush:
                on_flush(ctx.final_answer)
            self._publish(ctx, cache_key)
            return

        effective_setting = retrieval_setting
        if admission_action == "degraded_retrieval":
            if getattr(ctx, "_use_requested_setting", False):
                # Quality sweeps measure the named setting; the ladder's
                # TPM-pressure downgrade is recorded but not applied.
                ctx.record_event("admission_degrade_declined")
            else:
                effective_setting = "A-degraded"
        elif getattr(ctx, "_allow_lease_degrade", True) and admission_action == "normal" and effective_setting not in ("A-degraded",):
            # Lease-aware degradation: if the draft send would wait longer than
            # the degrade threshold on the measured TPM window, fetch fewer
            # passages instead of making the person wait (Phase A integration).
            est_full = int(len(question) / 3.8) + RETRIEVAL_SETTINGS[effective_setting].top_k * 350 + 600
            if await self.budget.peek_wait(MODEL_DRAFT, est_full) > LEASE_DEGRADE_WAIT_S:
                effective_setting = "A-degraded"
                ctx.record_event("lease_degrade")
        ctx.retrieval_setting = effective_setting

        # S1: Triage (fast model, JSON mode). Triage is an optimisation, not a
        # dependency: when its quota is gone, degrade to unscoped retrieval and
        # let the S3 score gate protect quality instead of failing the request.
        triage_admission, _, _ = await self.budget.check_admission(MODEL_TRIAGE)
        ctx.record_event("s1_start")
        if triage_admission == "budget_exhausted":
            ctx.record_event("s1_skipped_triage_quota")
            triage_result = {"classification": "answerable", "target_paper": "ALL", "queries": [question]}
        else:
            triage_result = await self._run_stage1_triage(ctx, question, retry_note)
        ctx.record_event("s1_end")

        classification = triage_result.get("classification", "answerable")
        target_paper = triage_result.get("target_paper", "ALL")

        # S3 gate (branch): refusal paths
        if classification in ("out_of_scope", "ambiguous"):
            ctx.record_event("s3_refuse")
            ctx.status = "ok"
            ctx.answer_grade = "refused_correctly"
            ctx.refusal_reason = f"triage_{classification}"
            ctx.final_answer = (
                f"Refusal: The question is {classification}. "
                "Our library contains only Attention Is All You Need, Moshi, and Dynamic Chunking."
            )
            ctx.first_token_ms = int((time.time() - ctx.enqueue_time) * 1000)
            ctx.total_ms = ctx.first_token_ms
            if on_flush:
                on_flush(ctx.final_answer)
            self._publish(ctx, cache_key)
            return

        # S2: Retrieve (local BM25 + optional rerank). Offloaded to a worker
        # thread: the cross-encoder predict is CPU-bound PyTorch and would
        # otherwise freeze the event loop for every concurrent request.
        ctx.record_event("s2_start")
        passages = await asyncio.to_thread(
            self.retrieval.search,
            question,
            target_paper,
            effective_setting,
        )
        passages = self._prepare_passages(passages, question)
        ctx.record_event("s2_end")

        best_score = passages[0].score if passages else 0.0
        setting_cfg = RETRIEVAL_SETTINGS.get(effective_setting)
        gate_threshold = (
            RERANK_GATE_THRESHOLD if (setting_cfg and setting_cfg.rerank) else SCORE_GATE_THRESHOLD
        )
        if best_score < gate_threshold:
            ctx.record_event("s3_refuse_low_score")
            ctx.status = "ok"
            ctx.answer_grade = "refused_correctly"
            ctx.refusal_reason = f"low_retrieval_score_{best_score:.2f}"
            ctx.final_answer = (
                f"Refusal: The research library does not contain sufficient evidence "
                f"(score {best_score:.2f} < {gate_threshold}) to answer accurately."
            )
            ctx.first_token_ms = int((time.time() - ctx.enqueue_time) * 1000)
            ctx.total_ms = ctx.first_token_ms
            if on_flush:
                on_flush(ctx.final_answer)
            self._publish(ctx, cache_key)
            return

        # S4 + S5: streaming draft with clause verification and recovery loop
        s4_retry_note = self._make_retry_note(ctx)
        await self._run_stage4_and_5_loop(
            ctx, question, passages, draft_model, on_flush, s4_retry_note
        )
        self._publish(ctx, cache_key)

    async def _run_stage1_triage(
        self, ctx: RequestContext, question: str, retry_note: Callable
    ) -> Dict:
        messages = [
            {"role": "system", "content": TRIAGE_SYSTEM_PROMPT},
            {"role": "user", "content": f"Classify this question:\n{question}"},
        ]

        full_content = ""
        tokens_in = 0
        tokens_out = 0

        try:
            async for chunk in self.client.stream_chat_completion(
                model=MODEL_TRIAGE,
                messages=messages,
                temperature=0.0,
                max_tokens=150,
                response_format=TRIAGE_JSON_FORMAT,
                on_retry=lambda reason, sleep_s=0.0: retry_note("s1", reason, sleep_s),
            ):
                full_content += chunk.delta_text
                if chunk.tokens_in:
                    tokens_in = chunk.tokens_in
                if chunk.tokens_out:
                    tokens_out = chunk.tokens_out
        except Exception as e:
            if type(e).__name__ == "RateLimitExceededError" or "Rate limited (429)" in str(e):
                # Triage quota is gone: degrade to unscoped retrieval rather
                # than failing the whole request (the draft may still work).
                # Daily exhaustion is not a concurrency signal; don't shrink.
                if getattr(e, "scope", "minute") == "minute":
                    self.limiter.record_rate_limited()
                ctx.record_event("s1_rate_limited_degrade")
                return {"classification": "answerable", "target_paper": "ALL", "queries": [question]}
            raise

        ctx.tokens_in += tokens_in
        ctx.tokens_out += tokens_out
        ctx.model_calls.append(
            {"stage": "s1", "model": MODEL_TRIAGE, "tokens_in": tokens_in, "tokens_out": tokens_out}
        )

        cleaned = full_content.strip()
        if "```json" in cleaned:
            cleaned = cleaned.split("```json")[1].split("```")[0].strip()
        elif "```" in cleaned:
            cleaned = cleaned.split("```")[1].split("```")[0].strip()
        try:
            return json.loads(cleaned)
        except Exception:
            # JSON mode should make this rare; never silently proceed without a trace.
            ctx.record_event("s1_parse_fallback")
            return {"classification": "answerable", "target_paper": "ALL", "queries": [question]}

    async def _run_stage4_and_5_loop(
        self,
        ctx: RequestContext,
        question: str,
        passages: List[RetrievedPassage],
        draft_model: str,
        on_flush: Optional[Callable[[str], None]] = None,
        retry_note: Optional[Callable] = None,
    ) -> None:
        max_loop_attempts = 2
        current_passages = passages

        for loop_attempt in range(1, max_loop_attempts + 1):
            if loop_attempt > 1:
                ctx.loop_triggered = True

            ctx.record_event(f"s4_start_attempt_{loop_attempt}")

            context_str = "\n\n".join(
                f"Passage ({p.citation_tag}):\n{p.text}" for p in current_passages
            )
            messages = [
                {"role": "system", "content": DRAFT_SYSTEM_PROMPT},
                {
                    "role": "user",
                    "content": f"Passages:\n{context_str}\n\nQuestion: {question}\n\nAnswer:",
                },
            ]

            clause_buffer = ""
            verified_output_chunks: List[str] = []
            provider_first_token_seen = False
            first_flush_done = False
            s5_failed_on_first_clause = False
            tokens_in = 0
            tokens_out = 0

            try:
                async for chunk in self.client.stream_chat_completion(
                    model=draft_model,
                    messages=messages,
                    temperature=0.0,
                    max_tokens=600,
                    on_retry=lambda reason, sleep_s=0.0: (
                        retry_note(f"s4_att{loop_attempt}", reason, sleep_s) if retry_note else None
                    ),
                ):
                    if not provider_first_token_seen and chunk.delta_text.strip():
                        ctx.record_event("s4_provider_first_token")
                        provider_first_token_seen = True

                    clause_buffer += chunk.delta_text
                    if chunk.tokens_in:
                        tokens_in = chunk.tokens_in
                    if chunk.tokens_out:
                        tokens_out = chunk.tokens_out

                    if re.search(r"[.!?\]]\s+$", clause_buffer) or len(clause_buffer) > 120:
                        is_valid, _ = self._verify_clause_citations(clause_buffer, current_passages)
                        if not is_valid and not first_flush_done:
                            s5_failed_on_first_clause = True
                            break

                        if not first_flush_done:
                            ctx.record_event("s5_first_flush")
                            ctx.first_token_ms = int((time.time() - ctx.enqueue_time) * 1000)
                            first_flush_done = True

                        verified_output_chunks.append(clause_buffer)
                        ctx.clauses_verified += 1
                        if on_flush:
                            on_flush(clause_buffer)
                        clause_buffer = ""

                ctx.tokens_in += tokens_in
                ctx.tokens_out += tokens_out
                ctx.model_calls.append(
                    {
                        "stage": f"s4_att_{loop_attempt}",
                        "model": draft_model,
                        "tokens_in": tokens_in,
                        "tokens_out": tokens_out,
                    }
                )

                if s5_failed_on_first_clause and loop_attempt < max_loop_attempts:
                    ctx.retries.append(
                        {
                            "stage": "s5_verify_retry",
                            "attempt": loop_attempt,
                            "reason": "citation_mismatch",
                            "rel_ms": int((time.time() - ctx.enqueue_time) * 1000),
                            "sleep_s": 0.0,
                        }
                    )
                    current_passages = await asyncio.to_thread(
                        self.retrieval.search,
                        question,
                        "ALL",
                        "A",
                    )
                    current_passages = self._prepare_passages(current_passages, question)
                    continue

                if clause_buffer:
                    is_valid, _ = self._verify_clause_citations(clause_buffer, current_passages)
                    if is_valid or first_flush_done:
                        verified_output_chunks.append(clause_buffer)
                        ctx.clauses_verified += 1
                        if on_flush:
                            on_flush(clause_buffer)
                        if not first_flush_done:
                            ctx.record_event("s5_first_flush")
                            ctx.first_token_ms = int((time.time() - ctx.enqueue_time) * 1000)
                            first_flush_done = True

                ctx.record_event(f"s4_end_attempt_{loop_attempt}")
                ctx.final_answer = "".join(verified_output_chunks).strip()

                if not ctx.final_answer:
                    ctx.final_answer = "Refusal: Could not construct a verified cited answer from the corpus."
                    ctx.refusal_reason = "no_verified_answer"
                    ctx.answer_grade = "refused_correctly"
                    if on_flush:
                        on_flush(ctx.final_answer)
                else:
                    ctx.answer_grade = "correct"

                if ctx.first_token_ms is not None and ctx.first_token_ms > 8000:
                    self.limiter.record_degraded()  # provider under strain
                else:
                    self.limiter.record_success()
                return

            except Exception as e:
                # Never retry into a provider 429 — quota exhaustion does not
                # heal by re-drafting; propagate for honest classification.
                # Only minute-window 429s shrink the limiter: daily exhaustion
                # is not a concurrency problem.
                is_rate_limit = type(e).__name__ == "RateLimitExceededError" or "Rate limited" in str(e)
                if is_rate_limit:
                    if getattr(e, "scope", "minute") == "minute":
                        self.limiter.record_rate_limited()
                    raise
                if loop_attempt < max_loop_attempts:
                    continue
                raise
