import asyncio
import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from src.relay.config import (
    DATA_DIR,
    LEASE_HEADROOM,
    MODEL_DRAFT,
    MODEL_TRIAGE,
    RPM_PRESSURE_DELAY_S,
)

BUDGET_FILE = DATA_DIR / "budget.json"

# Limits from real Groq free-tier measurements (logs/calibration.json, 2026-11-13):
# RPM=30 (429 body text), TPM=8,000 (x-ratelimit-limit-tokens header), RPD=1,000
# (x-ratelimit-limit-requests header) — all high confidence. TPD was not probed
# (measuring it consumes the whole daily budget); 200,000 is the provider's
# documented free-tier value for both models, treated as medium confidence.
DEFAULT_LIMITS = {
    MODEL_TRIAGE: {"rpm": 30, "rpd": 1000, "tpm": 8000, "tpd": 200000},
    MODEL_DRAFT: {"rpm": 30, "rpd": 1000, "tpm": 8000, "tpd": 200000},
}


def _new_entry() -> Dict[str, Any]:
    return {
        "rpd_used": 0,
        "tpd_used": 0,
        "rpm_window": [],
        "tpm_window": [],
        "hdr_rem_req": None,
        "hdr_rem_tok": None,
        "hdr_limit_req": None,
        "hdr_limit_tok": None,
        "hdr_itpm_limit": None,
        "hdr_otpm_limit": None,
        "hdr_reset_tok_s": 0.0,
        "hdr_ts": None,
    }


class _FileLock:
    """Cross-process advisory lock (O_EXCL create) so concurrent CLI processes
    cannot double-count usage. asyncio-friendly: acquired in a worker thread."""

    def __init__(self, path: Path, timeout_s: float = 5.0) -> None:
        self.path = path.with_suffix(path.suffix + ".lock")
        self.timeout_s = timeout_s

    def _acquire_sync(self) -> Any:
        deadline = time.monotonic() + self.timeout_s
        while True:
            try:
                return os.open(str(self.path), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
            except FileExistsError:
                if time.monotonic() > deadline:
                    # Stale lock from a crashed process: break it rather than hang.
                    try:
                        os.unlink(str(self.path))
                    except OSError:
                        pass
                    continue
                time.sleep(0.02)

    async def __aenter__(self) -> "_FileLock":
        self._fd = await asyncio.get_event_loop().run_in_executor(None, self._acquire_sync)
        return self

    async def __aexit__(self, *exc: Any) -> None:
        os.close(self._fd)
        try:
            os.unlink(str(self.path))
        except OSError:
            pass


class BudgetLedger:
    """Tracks per-model usage windows and observed provider limits, and runs
    the admission ladder plus the proactive send-lease that keeps us inside
    the measured RPM/TPM windows instead of reacting to 429s after the fact."""

    def __init__(
        self,
        path: Optional[Path] = None,
        limits: Optional[Dict[str, Dict[str, int]]] = None,
    ) -> None:
        self.path = path or BUDGET_FILE
        self.lock = asyncio.Lock()
        self.file_lock = _FileLock(self.path)
        self.limits = limits or DEFAULT_LIMITS
        self.data: Dict[str, Any] = self._load_initial_state()

    def _load_initial_state(self) -> Dict[str, Any]:
        today_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.path.exists():
            try:
                with open(self.path, "r", encoding="utf-8") as f:
                    data = json.load(f)
                    if data.get("date") == today_str:
                        for model in self.limits:
                            data["models"].setdefault(model, _new_entry())
                        return data
            except Exception:
                pass

        return {
            "date": today_str,
            "models": {m: _new_entry() for m in self.limits},
        }

    async def _flush_locked(self) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        async with self.file_lock:
            tmp = self.path.with_suffix(".tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self.data, f, indent=2)
            os.replace(tmp, self.path)

    def _prune_windows(self, model: str, now: float) -> None:
        entry = self.data["models"].setdefault(model, _new_entry())
        cutoff = now - 60.0
        entry["rpm_window"] = [t for t in entry["rpm_window"] if t > cutoff]
        # tpm entries are [t, tokens, lease_id?] — index, don't unpack.
        entry["tpm_window"] = [x for x in entry["tpm_window"] if x[0] > cutoff]

    def _observed_limits(self, model: str) -> Tuple[int, int, int, int]:
        """(rpm, tpm, rpd, tpd) — observed header values override assumptions."""
        entry = self.data["models"].get(model, {})
        lim = self.limits[model]
        rpm = lim["rpm"]
        tpm = entry.get("hdr_limit_tok") or lim["tpm"]
        rpd = entry.get("hdr_limit_req") or lim["rpd"]
        tpd = lim["tpd"]  # TPD is not exposed in provider headers
        return rpm, tpm, rpd, tpd

    # ---- Phase A1: proactive send lease -------------------------------

    async def await_send_slot(self, model: str, est_tokens: int, max_wait_s: float = 30.0) -> Tuple[Optional[str], float]:
        """Sleep until sending `est_tokens` fits inside the measured RPM/TPM
        windows with headroom, so we avoid 429s proactively. Books the send
        into the windows immediately (the provider counts a request from the
        moment it accepts it) and returns (lease_id, seconds_waited); the lease
        id lets record_usage reconcile the estimate with the actual token
        count. lease_id None means the wait budget was exhausted (caller
        proceeds and lets the provider decide)."""
        start = time.time()
        lease_id: Optional[str] = None
        while True:
            async with self.lock:
                now = time.time()
                self._prune_windows(model, now)
                entry = self.data["models"][model]
                rpm, tpm, rpd, tpd = self._observed_limits(model)
                rpm_count = len(entry["rpm_window"])
                tpm_used = sum(x[1] for x in entry["tpm_window"])
                rpm_free_at = (entry["rpm_window"][0] + 60.0) if rpm_count >= int(rpm * LEASE_HEADROOM) else 0.0
                token_free_at = 0.0
                if tpm_used + est_tokens > tpm * LEASE_HEADROOM and entry["tpm_window"]:
                    deficit = (tpm_used + est_tokens) - tpm * LEASE_HEADROOM
                    drain_per_s = tpm_used / 60.0
                    token_free_at = now + (deficit / drain_per_s if drain_per_s > 0 else 60.0)
                free_at = max(rpm_free_at, token_free_at)
            wait_s = free_at - time.time()
            if wait_s <= 0.02:
                async with self.lock:
                    now = time.time()
                    self._prune_windows(model, now)
                    e = self.data["models"][model]
                    lease_id = f"{model}:{now:.6f}"
                    e["rpm_window"].append(now)
                    e["tpm_window"].append([now, est_tokens, lease_id])
                return lease_id, time.time() - start
            if time.time() - start + wait_s > max_wait_s:
                return None, time.time() - start
            awaited = min(wait_s, max_wait_s - (time.time() - start))
            await asyncio.sleep(awaited)

    async def peek_wait(self, model: str, est_tokens: int) -> float:
        """Seconds the send-lease would wait right now (no booking, no sleeping).
        Used to degrade retrieval instead of waiting when the limit is close."""
        async with self.lock:
            now = time.time()
            self._prune_windows(model, now)
            entry = self.data["models"][model]
            rpm, tpm, _, _ = self._observed_limits(model)
            rpm_count = len(entry["rpm_window"])
            tpm_used = sum(x[1] for x in entry["tpm_window"])
            rpm_free_at = (entry["rpm_window"][0] + 60.0) if rpm_count >= int(rpm * LEASE_HEADROOM) else 0.0
            token_free_at = 0.0
            if tpm_used + est_tokens > tpm * LEASE_HEADROOM and entry["tpm_window"]:
                deficit = (tpm_used + est_tokens) - tpm * LEASE_HEADROOM
                drain_per_s = tpm_used / 60.0
                token_free_at = now + (deficit / drain_per_s if drain_per_s > 0 else 60.0)
            return max(0.0, max(rpm_free_at, token_free_at) - now)

    # -------------------------------------------------------------------

    async def record_usage(self, model: str, tokens_in: int, tokens_out: int, lease_id: Optional[str] = None) -> None:
        """Record actual usage. With a lease id, reconcile the booked estimate
        in place instead of appending (no double counting of RPM/TPM windows)."""
        async with self.lock:
            now = time.time()
            self._prune_windows(model, now)
            entry = self.data["models"][model]
            total_toks = tokens_in + tokens_out
            entry["rpd_used"] += 1
            entry["tpd_used"] += total_toks
            if lease_id:
                for x in entry["tpm_window"]:
                    if len(x) > 2 and x[2] == lease_id:
                        x[1] = total_toks
                        break
                # rpm_window was already counted when the lease was booked.
            else:
                entry["rpm_window"].append(now)
                entry["tpm_window"].append([now, total_toks])
            await self._flush_locked()

    async def hydrate_headers(self, model: str, headers: Dict[str, str]) -> None:
        """Adopt the provider's observed limits from rate-limit headers.

        The brief warns limits 'can change without notice', so observed header
        limits override the assumed DEFAULT_LIMITS on the next admission check.
        """
        async with self.lock:
            now = time.time()
            self._prune_windows(model, now)
            entry = self.data["models"].setdefault(model, _new_entry())

            lower_headers = {k.lower(): v for k, v in headers.items()}
            for header, field in [
                ("x-ratelimit-limit-requests", "hdr_limit_req"),
                ("x-ratelimit-limit-tokens", "hdr_limit_tok"),
                ("x-ratelimit-remaining-requests", "hdr_rem_req"),
                ("x-ratelimit-remaining-tokens", "hdr_rem_tok"),
            ]:
                if header in lower_headers:
                    try:
                        entry[field] = int(lower_headers[header])
                    except ValueError:
                        pass
            # Some plans expose split input/output token windows (docs: ITPM/OTPM).
            for header, field in [
                ("x-ratelimit-limit-input-tokens", "hdr_itpm_limit"),
                ("x-ratelimit-limit-output-tokens", "hdr_otpm_limit"),
            ]:
                if header in lower_headers:
                    try:
                        entry[field] = int(lower_headers[header])
                    except ValueError:
                        pass

            if "x-ratelimit-reset-tokens" in lower_headers:
                val = lower_headers["x-ratelimit-reset-tokens"]
                try:
                    if val.endswith("s"):
                        entry["hdr_reset_tok_s"] = float(val[:-1])
                    elif val.endswith("m"):
                        entry["hdr_reset_tok_s"] = float(val[:-1]) * 60.0
                    else:
                        entry["hdr_reset_tok_s"] = float(val)
                except ValueError:
                    pass
            # Per-minute snapshots (remaining tokens) reset every window; record
            # when they were taken so admission never acts on a stale one.
            entry["hdr_ts"] = now

            await self._flush_locked()

    async def mark_daily_exhausted(self, model: str) -> None:
        """Learn from a per-day 429 that the model's day is over: subsequent
        admission checks refuse immediately instead of letting requests run
        the pipeline and fail at the draft call."""
        async with self.lock:
            entry = self.data["models"].setdefault(model, _new_entry())
            now = datetime.now(timezone.utc)
            end_of_day = datetime(now.year, now.month, now.day, 23, 59, 59, tzinfo=timezone.utc).timestamp()
            entry["daily_exhausted_until"] = end_of_day
            await self._flush_locked()

    async def check_admission(self, model: str) -> Tuple[str, Optional[float], str]:
        """Admission ladder. Returns (action, delay_s, model_override):
        'normal' | 'degraded_retrieval' | 'fallback_model' | 'budget_exhausted'."""
        async with self.lock:
            now = time.time()
            self._prune_windows(model, now)
            entry = self.data["models"].setdefault(model, _new_entry())
            rpm, tpm, rpd, tpd = self._observed_limits(model)

            rpd_used = entry.get("rpd_used", 0)
            tpd_used = entry.get("tpd_used", 0)

            learned_daily = entry.get("daily_exhausted_until") or 0.0
            if learned_daily and now < learned_daily:
                return "budget_exhausted", None, model

            if rpd_used >= rpd or tpd_used >= tpd:
                return "budget_exhausted", None, model

            if rpd_used >= 0.90 * rpd or tpd_used >= 0.90 * tpd:
                return "fallback_model", None, MODEL_TRIAGE

            rpm_count = len(entry["rpm_window"])
            tpm_tokens = sum(x[1] for x in entry["tpm_window"])

            # hdr_rem_tok is a per-minute snapshot: only trust it while fresh.
            hdr_ts = entry.get("hdr_ts") or 0.0
            hdr_fresh = (now - hdr_ts) < 75.0
            hdr_rem_tok = entry.get("hdr_rem_tok") if hdr_fresh else None
            if hdr_rem_tok is not None and hdr_rem_tok < 1500:
                return "fallback_model", None, MODEL_TRIAGE
            if hdr_rem_tok is not None and hdr_rem_tok < 3500:
                return "degraded_retrieval", None, model

            if tpm_tokens >= 0.80 * tpm:
                return "degraded_retrieval", None, model

            if rpm_count >= 0.80 * rpm:
                delay = entry.get("hdr_reset_tok_s", RPM_PRESSURE_DELAY_S)
                return "normal", max(0.5, delay), model

            return "normal", None, model
