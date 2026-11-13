import asyncio
import json
import os
import random
import re
import time
from dataclasses import dataclass
from typing import AsyncGenerator, Callable, Dict, List, Optional

import httpx
from dotenv import load_dotenv

from src.relay.budget import BudgetLedger
from src.relay.config import (
    LOGS_DIR,
    TIMEOUT_CONNECT_S,
    TIMEOUT_INTER_TOKEN_S,
    TIMEOUT_TOTAL_S,
    TIMEOUT_TTFT_S,
)

load_dotenv()

GROQ_BASE_URL = "https://api.groq.com/openai/v1"
CALIBRATION_FILE = LOGS_DIR / "calibration.json"

# 429 bodies that name a per-day or per-minute window. Daily exhaustion never
# heals within the day (Phase A2): retrying into it is pure waste, so it is
# raised immediately; minute-window hits are transient and retryable.
_DAILY_LIMIT_PATTERN = re.compile(r"per day|daily|RPD|TPD", re.IGNORECASE)


@dataclass
class StreamChunk:
    delta_text: str
    tokens_in: Optional[int] = None
    tokens_out: Optional[int] = None
    first_token: bool = False


class RateLimitExceededError(RuntimeError):
    """Provider 429 that survived the retry budget — classify as rate_limited."""

    def __init__(self, message: str, scope: str = "minute") -> None:
        super().__init__(message)
        self.scope = scope  # "minute" | "day"


class TTFTTimeoutError(TimeoutError):
    pass


class GroqClient:
    def __init__(
        self,
        api_key: Optional[str] = None,
        base_url: str = GROQ_BASE_URL,
        budget_ledger: Optional[BudgetLedger] = None,
        source_name: str = "real",
    ) -> None:
        self.api_key = api_key or os.getenv("GROQ_API_KEY", "")
        self.base_url = base_url.rstrip("/")
        self.budget_ledger = budget_ledger
        self.source_name = source_name
        self.consecutive_429_count: Dict[str, int] = {}
        self.model_cooldown_until: Dict[str, float] = {}
        # One pooled client for the client's lifetime: TCP+TLS reused across
        # calls instead of a fresh handshake per model call.
        self._http: Optional[httpx.AsyncClient] = None

    def _http_client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(
                timeout=httpx.Timeout(
                    connect=TIMEOUT_CONNECT_S,
                    read=TIMEOUT_INTER_TOKEN_S,
                    write=TIMEOUT_CONNECT_S,
                    pool=TIMEOUT_CONNECT_S,
                ),
                limits=httpx.Limits(max_connections=32, max_keepalive_connections=16),
                headers=self._get_headers(),
            )
        return self._http

    async def aclose(self) -> None:
        if self._http is not None:
            await self._http.aclose()
            self._http = None

    def _get_headers(self) -> Dict[str, str]:
        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        return headers

    async def _record_calibration_429(
        self, model: str, status_code: int, headers: Dict[str, str], body_text: str
    ) -> None:
        if self.source_name != "real":
            return  # only real-provider evidence belongs in the calibration artifact
        CALIBRATION_FILE.parent.mkdir(parents=True, exist_ok=True)
        try:
            current_data = []
            if CALIBRATION_FILE.exists():
                with open(CALIBRATION_FILE, "r", encoding="utf-8") as f:
                    current_data = json.load(f)
            if not isinstance(current_data, list):
                current_data = [current_data]
            current_data.append(
                {
                    "timestamp": time.time(),
                    "model": model,
                    "status_code": status_code,
                    "headers": dict(headers),
                    "body": body_text,
                }
            )
            with open(CALIBRATION_FILE, "w", encoding="utf-8") as f:
                json.dump(current_data, f, indent=2)
        except Exception:
            pass

    async def stream_chat_completion(
        self,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float = 0.0,
        max_tokens: int = 1024,
        response_format: Optional[Dict] = None,
        max_retries: int = 2,
        on_retry: Optional[Callable[[str, float], None]] = None,
    ) -> AsyncGenerator[StreamChunk, None]:
        url = f"{self.base_url}/chat/completions"
        payload = {
            "model": model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
            "stream": True,
            "stream_options": {"include_usage": True},
        }
        if response_format is not None:
            payload["response_format"] = response_format

        # Phase A1: book the send against the measured windows *before* firing,
        # so predictable RPM/TPM pressure becomes queue delay, not 429 retries.
        lease_id = None
        if self.budget_ledger is not None:
            prompt_chars = sum(len(m.get("content", "")) for m in messages)
            est_tokens = int(prompt_chars / 3.8) + max_tokens
            lease_id, lease_waited_s = await self.budget_ledger.await_send_slot(model, est_tokens)
            if lease_waited_s > 0.05 and on_retry:
                # Throttle wait is 'waiting', not 'retrying' — the waterfall
                # splits buckets on the reason prefix.
                on_retry("lease_wait", lease_waited_s)

        now = time.time()
        cooldown = self.model_cooldown_until.get(model, 0.0)
        if now < cooldown:
            await asyncio.sleep(min(cooldown - now, 2.0))

        attempt = 0
        backoff = 0.5

        while attempt <= max_retries:
            attempt += 1
            try:
                # Phase D1: enforce the documented total-request timeout.
                async with asyncio.timeout(TIMEOUT_TOTAL_S):
                    async with self._http_client().stream("POST", url, json=payload) as response:
                        if self.budget_ledger:
                            await self.budget_ledger.hydrate_headers(model, dict(response.headers))

                        if response.status_code == 429:
                            body_text = (await response.aread()).decode("utf-8", errors="replace")
                            await self._record_calibration_429(
                                model, 429, dict(response.headers), body_text
                            )
                            self.consecutive_429_count[model] = (
                                self.consecutive_429_count.get(model, 0) + 1
                            )

                            retry_after = 2.0
                            raw_retry = response.headers.get("retry-after")
                            if raw_retry:
                                try:
                                    retry_after = float(raw_retry)
                                except ValueError:
                                    pass

                            scope = "day" if _DAILY_LIMIT_PATTERN.search(body_text) else "minute"
                            if scope == "day":
                                # Learn that the model's day is over so later
                                # requests are refused instantly at admission.
                                if self.budget_ledger is not None:
                                    await self.budget_ledger.mark_daily_exhausted(model)
                                # Never retry into a daily limit (Phase A2).
                                raise RateLimitExceededError(
                                    f"Rate limited (429, daily): {body_text}", scope="day"
                                )

                            if self.consecutive_429_count[model] >= 2:
                                self.model_cooldown_until[model] = time.time() + retry_after

                            if attempt > max_retries:
                                raise RateLimitExceededError(
                                    f"Rate limited (429): {body_text}", scope="minute"
                                )

                            sleep_time = retry_after + random.uniform(0.0, 0.5)
                            if on_retry:
                                on_retry("429_backoff", sleep_time)
                            await asyncio.sleep(sleep_time)
                            continue

                        if response.status_code >= 500:
                            body = (await response.aread()).decode("utf-8", errors="replace")
                            if attempt > max_retries:
                                raise RuntimeError(f"Server error ({response.status_code}): {body}")
                            sleep_time = backoff + random.uniform(0.0, 0.5)
                            if on_retry:
                                on_retry(f"5xx_backoff_{response.status_code}", sleep_time)
                            backoff *= 2.0
                            await asyncio.sleep(sleep_time)
                            continue

                        if response.status_code != 200:
                            body = (await response.aread()).decode("utf-8", errors="replace")
                            raise RuntimeError(f"API error ({response.status_code}): {body}")

                        self.consecutive_429_count[model] = 0
                        first_token = True
                        tokens_in = None
                        tokens_out = None
                        ttft_deadline = time.monotonic() + TIMEOUT_TTFT_S

                        async for line in response.aiter_lines():
                            line = line.strip()
                            if not line or not line.startswith("data: "):
                                continue
                            data_str = line[6:]
                            if data_str == "[DONE]":
                                break

                            try:
                                chunk_json = json.loads(data_str)
                            except json.JSONDecodeError:
                                continue

                            usage = chunk_json.get("usage")
                            if usage:
                                tokens_in = usage.get("prompt_tokens")
                                tokens_out = usage.get("completion_tokens")

                            choices = chunk_json.get("choices", [])
                            delta = choices[0].get("delta", {}) if choices else {}
                            content = delta.get("content", "") or ""

                            if content:
                                if first_token and time.monotonic() > ttft_deadline:
                                    # The provider accepted the request but never
                                    # produced a first token in time (Phase D1).
                                    raise TTFTTimeoutError(
                                        f"TTFT exceeded {TIMEOUT_TTFT_S:.0f}s on {model}"
                                    )
                                yield StreamChunk(
                                    delta_text=content,
                                    tokens_in=tokens_in,
                                    tokens_out=tokens_out,
                                    first_token=first_token,
                                )
                                first_token = False
                            elif tokens_in is not None and tokens_out is not None:
                                # Usage-only terminator chunk (OpenAI-style final
                                # chunk or choices-less usage chunk).
                                yield StreamChunk(
                                    delta_text="",
                                    tokens_in=tokens_in,
                                    tokens_out=tokens_out,
                                    first_token=False,
                                )

                        if self.budget_ledger and tokens_in and tokens_out:
                            await self.budget_ledger.record_usage(
                                model, tokens_in, tokens_out, lease_id=lease_id
                            )

                        return

            except TTFTTimeoutError as e:
                if attempt > max_retries:
                    raise RuntimeError(f"TTFT timeout after {attempt} attempts: {e}")
                sleep_time = backoff + random.uniform(0.0, 0.5)
                if on_retry:
                    on_retry("ttft_timeout", sleep_time)
                backoff *= 2.0
                await asyncio.sleep(sleep_time)

            except asyncio.TimeoutError as e:
                if attempt > max_retries:
                    raise RuntimeError(f"Total request timeout ({TIMEOUT_TOTAL_S:.0f}s): {e}")
                sleep_time = backoff + random.uniform(0.0, 0.5)
                if on_retry:
                    on_retry("total_timeout", sleep_time)
                backoff *= 2.0
                await asyncio.sleep(sleep_time)

            except (httpx.TimeoutException, httpx.NetworkError) as e:
                if attempt > max_retries:
                    raise RuntimeError(f"Request timeout / network error: {e}")
                sleep_time = backoff + random.uniform(0.0, 0.5)
                if on_retry:
                    on_retry(f"network_{type(e).__name__}", sleep_time)
                backoff *= 2.0
                await asyncio.sleep(sleep_time)
