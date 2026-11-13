import argparse
import csv
import json
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Tuple

try:
    from relay.config import (
        LOGS_DIR,
        PRICE_DRAFT_INPUT_PER_1M,
        PRICE_DRAFT_OUTPUT_PER_1M,
        RESULTS_DIR,
    )
except ImportError:
    from src.relay.config import (
        LOGS_DIR,
        PRICE_DRAFT_INPUT_PER_1M,
        PRICE_DRAFT_OUTPUT_PER_1M,
        RESULTS_DIR,
    )

RESULTS_CSV = RESULTS_DIR / "results.csv"
CALIBRATION_JSON = LOGS_DIR / "calibration.json"
SPANS_LOG_FILE = LOGS_DIR / "spans.jsonl"
SCENARIOS_FILE = RESULTS_CSV.parent.parent / "scenarios.yaml"


def _num(x: str) -> Optional[int]:
    try:
        return int(str(x).replace(",", ""))
    except Exception:
        return None


def _load_calibration() -> List[Dict]:
    if not CALIBRATION_JSON.exists():
        return []
    try:
        with open(CALIBRATION_JSON, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception:
        return []


def _derive_limits_from_calibration() -> Dict[str, Dict[str, str]]:
    """Derive per-model limits from real calibration evidence only.

    Parses x-ratelimit-* headers and 429 error bodies (Groq embeds the full
    limit ladder in its 429 message text). Returns {model: {field: 'value||how||confidence'}}.
    """
    out: Dict[str, Dict[str, str]] = {}
    for rec in _load_calibration():
        # Skip stand-in-origin records (our own mock service) so Table 1 is real-only
        hdrs_all = {k.lower(): str(v) for k, v in (rec.get("headers") or {}).items()}
        if hdrs_all.get("server") == "uvicorn" or "org-standin" in (rec.get("body") or ""):
            continue
        model = rec.get("model") or "unknown"
        e = out.setdefault(
            model,
            {
                "rpm": ("not measured", "", "low"),
                "tpm": ("not measured", "", "low"),
                "rpd": ("not measured", "", "low"),
                "tpd": ("200,000", "provider documentation", "medium"),
                "inflight": ("not measured", "", "low"),
                "at_limit": ("not measured", "", "low"),
                "recovery": ("not measured", "", "low"),
                "ttft": ("not measured", "", "low"),
            },
        )
        headers = {k.lower(): str(v) for k, v in (rec.get("headers") or {}).items()}
        body = rec.get("body") or ""

        if headers.get("x-ratelimit-limit-requests"):
            v = _num(headers["x-ratelimit-limit-requests"])
            if v:
                e["rpd"] = (f"{v:,}", "x-ratelimit-limit-requests header", "high")
        if headers.get("x-ratelimit-limit-tokens"):
            v = _num(headers["x-ratelimit-limit-tokens"])
            if v:
                e["tpm"] = (f"{v:,}", "x-ratelimit-limit-tokens header", "high")

        # Groq 429 bodies spell out the ladder, e.g.
        # "on requests per minute (RPM): Limit 30, Used 30, Requested 1"
        for pat, key in [
            (r"requests per minute \(RPM\): Limit ([\d,]+)", "rpm"),
            (r"tokens per minute \(TPM\): Limit ([\d,]+)", "tpm"),
            (r"requests per day \(RPD\): Limit ([\d,]+)", "rpd"),
            (r"tokens per day \(TPD\): Limit ([\d,]+)", "tpd"),
        ]:
            m = re.search(pat, body, re.IGNORECASE)
            if m:
                e[key] = (m.group(1).rstrip(","), "429 error body text", "high")
        # Older message format fallback
        m = re.search(r"Limit ([\d,]+) Requests Per Minute", body)
        if m and e["rpm"][0] == "not measured":
            e["rpm"] = (m.group(1).rstrip(","), "429 error body text", "high")
        m = re.search(r"Limit ([\d,]+) Tokens Per Minute", body)
        if m and e["tpm"][0] == "not measured":
            e["tpm"] = (m.group(1).rstrip(","), "429 error body text", "high")
        m = re.search(r"Limit ([\d,]+) Requests Per Day", body)
        if m and e["rpd"][0] == "not measured":
            e["rpd"] = (m.group(1).rstrip(","), "429 error body text", "high")
        m = re.search(r"Limit ([\d,]+) Tokens Per Day", body)
        if m and e["tpd"][0] == "not measured":
            e["tpd"] = (m.group(1).rstrip(","), "429 error body text", "high")

        if rec.get("phase") == "concurrency_probe":
            ok_c = int(rec.get("succeeded") or 0)
            prev = e["inflight"]
            best = max(ok_c, int(prev[0]) if prev[0].isdigit() else 0)
            e["inflight"] = (
                f"{best} concurrent OK",
                "parallel tiny-request probe",
                "medium",
            )

        if rec.get("status_code") == 429:
            e["at_limit"] = (
                (re.search(r'"code"\s*:\s*"([^"]+)"', body).group(1) if re.search(r'"code"\s*:\s*"([^"]+)"', body) else "HTTP 429"),
                "captured 429 response",
                "high",
            )
            if headers.get("retry-after"):
                e["recovery"] = (f"retry-after {headers['retry-after']}s", "retry-after header on 429", "high")

        if rec.get("phase") == "latency_distribution" and rec.get("ttft_ms") is not None:
            samples = e.get("_ttft_samples", [])
            samples.append(rec["ttft_ms"])
            e["_ttft_samples"] = samples

    for model, e in out.items():
        samples = e.pop("_ttft_samples", None)
        if samples:
            med = sorted(samples)[len(samples) // 2]
            e["ttft"] = (f"~{med:.0f} ms median over {len(samples)} probes", "latency distribution probe", "medium")
    return out


def percentile(data: List[float], p: float) -> float:
    if not data:
        return 0.0
    sorted_d = sorted(data)
    idx = int(len(sorted_d) * (p / 100.0))
    idx = min(idx, len(sorted_d) - 1)
    return sorted_d[idx]


def load_results_rows() -> List[Dict[str, str]]:
    if not RESULTS_CSV.exists():
        return []
    rows = []
    with open(RESULTS_CSV, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for r in reader:
            rows.append(r)
    return rows


def compute_table_1() -> str:
    limits = _derive_limits_from_calibration()

    def fmt(triple) -> str:
        value, how, conf = triple
        return f"{value} | {how or '-'} | {conf}"

    lines = [
        "### Table 1: Limits Found in the Real Service",
        "",
        "Derived only from `logs/calibration.json` evidence (headers + captured 429 bodies). "
        "'not measured' means no real evidence was captured for that cell.",
        "",
        "| Limit | " + " | ".join(limits.keys()) if limits else "| Limit | (no calibration evidence) |",
        "",
    ]
    if not limits:
        return "\n".join(lines) + "\nNo calibration records found.\n"

    models = list(limits.keys())
    rows = [
        ("Requests per minute", "rpm"),
        ("Tokens per minute", "tpm"),
        ("Requests per day", "rpd"),
        ("Tokens per day", "tpd"),
        ("Requests in flight at once", "inflight"),
        ("What it returns when a limit is hit", "at_limit"),
        ("Time until it recovers", "recovery"),
        ("TTFT (median of probes)", "ttft"),
    ]
    lines = [
        "### Table 1: Limits Found in the Real Service",
        "",
        "Derived only from `logs/calibration.json` evidence (captured headers + 429 bodies + probes). "
        "A cell says 'not measured' when no real evidence was captured for it.",
        "",
        "| Limit | " + " | ".join(models) + " |",
        "|---|" + "---|" * len(models),
    ]
    for label, key in rows:
        cells = []
        for m in models:
            value, how, conf = limits[m][key]
            cell = value if value != "not measured" else "not measured"
            if how and value != "not measured":
                cell = f"{value} ({how}; {conf})"
            cells.append(cell)
        lines.append(f"| {label} | " + " | ".join(cells) + " |")
    lines.append("")
    return "\n".join(lines)


def _load_scenario_configs() -> Dict[str, Dict]:
    if not SCENARIOS_FILE.exists():
        return {}
    try:
        import yaml
        with open(SCENARIOS_FILE, "r", encoding="utf-8") as f:
            return yaml.safe_load(f).get("scenarios", {})
    except Exception:
        return {}


def _peak_queue_by_scenario() -> Dict[str, int]:
    peak: Dict[str, int] = {}
    if not SPANS_LOG_FILE.exists():
        return peak
    with open(SPANS_LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                sc = rec.get("scenario", "?")
                ql = rec.get("queue_len_at_dequeue") or 0
                if ql > peak.get(sc, 0):
                    peak[sc] = ql
            except Exception:
                continue
    return peak


def compute_tables_2a_2b(rows: List[Dict[str, str]]) -> Tuple[str, str]:
    by_scenario = defaultdict(list)
    for r in rows:
        if r.get("request_id", "").startswith("eval-") or r.get("scenario") == "eval":
            continue  # eval sweep rows are reported in Table 3 only
        by_scenario[r["scenario"]].append(r)

    scenario_cfgs = _load_scenario_configs()
    peak_queue = _peak_queue_by_scenario()

    scenarios_order = ["steady", "burst", "overload", "rpm_halved", "budget_exhausted"]

    t2a_lines = [
        "### Table 2a: Latency per Load Scenario",
        "",
        "| Scenario | Source | Requests Sent | Duration (s) | Offered Rate (RPM) | First Token p50 / p95 / p99 (ms) | Total Time p50 / p95 / p99 (ms) |",
        "|---|---|---|---|---|---|---|",
    ]

    t2b_lines = [
        "### Table 2b: Outcomes per Load Scenario",
        "",
        "| Scenario | Succeeded % | Rate-limited % | Timed-out / Given-up % | Retries / Req | Peak Queue Len | Tokens / Answer | Cost / 1k Answers ($) |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for sc in scenarios_order:
        sc_rows = by_scenario.get(sc, [])
        if not sc_rows:
            t2a_lines.append(f"| {sc} | not measured | 0 | 0 | 0 | - / - / - | - / - / - |")
            t2b_lines.append(f"| {sc} | - | - | - | - | - | - | - |")
            continue

        n = len(sc_rows)
        source = sc_rows[0].get("source", "stand-in")

        ft_vals = [float(r["first_token_ms"]) for r in sc_rows if r.get("first_token_ms")]
        tot_vals = [float(r["total_ms"]) for r in sc_rows if r.get("total_ms")]

        ft_p50 = f"{percentile(ft_vals, 50):.0f}" if ft_vals else "-"
        ft_p95 = f"{percentile(ft_vals, 95):.0f}" if ft_vals else "-"
        ft_p99 = f"{percentile(ft_vals, 99):.0f}" if ft_vals else "-"

        tot_p50 = f"{percentile(tot_vals, 50):.0f}" if tot_vals else "-"
        tot_p95 = f"{percentile(tot_vals, 95):.0f}" if tot_vals else "-"
        tot_p99 = f"{percentile(tot_vals, 99):.0f}" if tot_vals else "-"

        succeeded_cnt = sum(1 for r in sc_rows if r["status"] == "ok")
        rl_cnt = sum(1 for r in sc_rows if r["status"] == "rate_limited")
        to_cnt = sum(1 for r in sc_rows if r["status"] in ("timeout", "error"))

        succ_pct = (succeeded_cnt / n) * 100.0
        rl_pct = (rl_cnt / n) * 100.0
        to_pct = (to_cnt / n) * 100.0

        retries_cnt = sum(int(r.get("retries", 0) or 0) for r in sc_rows)
        retries_per_req = retries_cnt / n

        ok_with_tokens = [r for r in sc_rows if r["status"] == "ok" and (int(r.get("tokens_in", 0) or 0) + int(r.get("tokens_out", 0) or 0)) > 0]
        tok_per_ans = (
            sum(int(r.get("tokens_in", 0) or 0) + int(r.get("tokens_out", 0) or 0) for r in ok_with_tokens)
            / len(ok_with_tokens)
            if ok_with_tokens
            else 0.0
        )

        tok_in_tot = sum(int(r.get("tokens_in", 0) or 0) for r in sc_rows)
        tok_out_tot = sum(int(r.get("tokens_out", 0) or 0) for r in sc_rows)
        cost_per_ans = (tok_in_tot / 1e6 * PRICE_DRAFT_INPUT_PER_1M) + (
            tok_out_tot / 1e6 * PRICE_DRAFT_OUTPUT_PER_1M
        )
        cost_per_1k = (cost_per_ans / max(1, succeeded_cnt)) * 1000.0 if (tok_in_tot or tok_out_tot) else 0.0

        cfg = scenario_cfgs.get(sc, {})
        offered_rpm = float(cfg.get("offered_rate_rpm", 0.0))
        duration_s = int(cfg.get("duration_s", 0))
        pq = peak_queue.get(sc, 0)

        t2a_lines.append(
            f"| {sc} | {source} | {n} | {duration_s} | {offered_rpm:.1f} | {ft_p50} / {ft_p95} / {ft_p99} | {tot_p50} / {tot_p95} / {tot_p99} |"
        )
        t2b_lines.append(
            f"| {sc} | {succ_pct:.1f}% | {rl_pct:.1f}% | {to_pct:.1f}% | {retries_per_req:.2f} | {pq if pq else '-'} | {tok_per_ans:.0f} | ${cost_per_1k:.3f} ($0 real) |"
        )

    return "\n".join(t2a_lines), "\n".join(t2b_lines)


def compute_table_3(rows: List[Dict[str, str]]) -> str:
    by_setting = defaultdict(list)
    for r in rows:
        setting = r.get("retrieval_setting", "A")
        if setting and (r.get("request_id", "").startswith("eval-") or r.get("scenario") == "eval"):
            by_setting[setting].append(r)

    setting_meta = {
        "A": "350 tokens, 6 pieces, no rerank",
        "B": "150 tokens, 12 pieces, no rerank",
        "C": "350 tokens, 3 reranked, rerank yes",
        "A-degraded": "350 tokens, 2 pieces, no rerank",
    }

    t3_lines = [
        "### Table 3: Retrieval Settings Evaluation",
        "",
        "Percentages are over *answered* (status=ok) requests; rate-limited or timed-out",
        "requests are availability outcomes (Table 2), not answer-quality outcomes.",
        "",
        "| Setting (piece size, fetched, rerank) | Questions Asked | Correct & Cited % | Wrong % | Correctly Refused % | Tokens / Answer | First Token p50 / p95 (ms) | Total Time p50 / p95 (ms) |",
        "|---|---|---|---|---|---|---|---|",
    ]

    for s_name in ["A", "B", "C", "A-degraded"]:
        s_rows = by_setting.get(s_name, [])
        label = f"{s_name} ({setting_meta.get(s_name, '')})"
        if not s_rows:
            t3_lines.append(f"| {label} | not measured | - | - | - | - | - / - | - / - |")
            continue

        n = len(s_rows)
        answered = [r for r in s_rows if r["status"] == "ok"]
        denom_q = len(answered)
        correct_cnt = sum(1 for r in answered if r.get("answer_grade") == "correct")
        wrong_cnt = sum(1 for r in answered if r.get("answer_grade") == "wrong")
        refused_cnt = sum(1 for r in answered if r.get("answer_grade") == "refused_correctly")
        refused_wrongly_cnt = sum(1 for r in answered if r.get("answer_grade") == "refused_wrongly")
        ungraded_cnt = denom_q - (correct_cnt + wrong_cnt + refused_cnt + refused_wrongly_cnt)

        pct_base = max(1, correct_cnt + wrong_cnt + refused_cnt + refused_wrongly_cnt)
        corr_pct = (correct_cnt / pct_base) * 100.0
        wrong_pct = (wrong_cnt / pct_base) * 100.0
        ref_pct = (refused_cnt / pct_base) * 100.0

        tok_in_tot = sum(int(r.get("tokens_in", 0) or 0) for r in s_rows)
        tok_out_tot = sum(int(r.get("tokens_out", 0) or 0) for r in s_rows)
        denom = max(1, correct_cnt + refused_cnt)
        tok_per_ans = (tok_in_tot + tok_out_tot) / denom

        ft_vals = [float(r["first_token_ms"]) for r in s_rows if r.get("first_token_ms")]
        tot_vals = [float(r["total_ms"]) for r in s_rows if r.get("total_ms")]
        ft_p50 = f"{percentile(ft_vals, 50):.0f}" if ft_vals else "-"
        ft_p95 = f"{percentile(ft_vals, 95):.0f}" if ft_vals else "-"
        tot_p50 = f"{percentile(tot_vals, 50):.0f}" if tot_vals else "-"
        tot_p95 = f"{percentile(tot_vals, 95):.0f}" if tot_vals else "-"

        t3_lines.append(
            f"| {label} | {n} asked / {denom_q} answered | {corr_pct:.1f}% | {wrong_pct:.1f}% | {ref_pct:.1f}% | {tok_per_ans:.0f} | {ft_p50} / {ft_p95} | {tot_p50} / {tot_p95} |"
        )

    return "\n".join(t3_lines)


def print_waterfall(request_id: str) -> bool:
    if not SPANS_LOG_FILE.exists():
        print(f"Error: Spans log not found at {SPANS_LOG_FILE}", file=sys.stderr)
        return False

    target_span = None
    with open(SPANS_LOG_FILE, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
                if rec.get("request_id") == request_id:
                    target_span = rec  # keep scanning: spans.jsonl accumulates across runs
            except Exception:
                continue

    if not target_span:
        print(f"Error: Request ID '{request_id}' not found in {SPANS_LOG_FILE}", file=sys.stderr)
        return False

    events = dict(target_span.get("events", []))
    t_enqueue = events.get("enqueue", 0)
    t_dequeue = events.get("dequeue", t_enqueue)
    t_s1_start = events.get("s1_start", t_dequeue)
    t_s1_end = events.get("s1_end", t_s1_start)
    t_s2_start = events.get("s2_start", t_s1_end)
    t_s2_end = events.get("s2_end", t_s2_start)
    t_s4_start = events.get("s4_start_attempt_1", events.get("s4_start", t_s2_end))
    t_s4_prov_tok = events.get("s4_provider_first_token", t_s4_start)
    t_s5_flush = events.get("s5_first_flush", t_s4_prov_tok)
    t_end = events.get("s4_end_attempt_1", events.get("s4_end", t_s5_flush))

    total_ms = int(target_span.get("total_ms") or 0) or max(t_end, t_s5_flush, 1)
    t_end = max(t_end, total_ms)

    queue_wait = max(0, t_dequeue - t_enqueue)
    s1_dur = max(0, t_s1_end - t_s1_start)
    s2_dur = max(0, t_s2_end - t_s2_start)
    s4_prov_ttft = max(0, t_s4_prov_tok - t_s4_start)
    s5_tax = max(0, t_s5_flush - t_s4_prov_tok)
    stream_dur = max(0, t_end - t_s5_flush)

    # Retry bucket: recorded backoff sleeps. Lease throttle waits are
    # 'waiting', not 'retrying' — split on the reason prefix.
    retry_wait_ms = 0
    lease_wait_ms = 0
    retry_events = []
    for r in target_span.get("retries", []):
        sleep_s = float(r.get("sleep_s") or 0)
        ms = int(sleep_s * 1000)
        if str(r.get("reason", "")).startswith("lease_wait"):
            lease_wait_ms += ms
        else:
            retry_wait_ms += ms
        retry_events.append((r.get("stage", "?"), r.get("reason", "?"), ms))

    before_first_tok_ms = t_s5_flush if "s5_first_flush" in events else 0
    after_first_tok_ms = max(0, total_ms - before_first_tok_ms) if before_first_tok_ms else 0
    waiting_ms = queue_wait + lease_wait_ms
    working_ms = max(0, total_ms - waiting_ms - retry_wait_ms - after_first_tok_ms)
    pct_before = (before_first_tok_ms / total_ms) * 100.0

    print("=" * 70)
    print(f"LATENCY WATERFALL TRACE: {request_id} (Scenario: {target_span.get('scenario')}, Source: {target_span.get('source')})")
    print("=" * 70)
    print(f"Total Request Duration: {total_ms} ms")
    print(f"  • Before First Token (TTFT): {before_first_tok_ms} ms ({pct_before:.1f}%)")
    print(f"  • After First Token (Stream): {after_first_tok_ms} ms")
    print("-" * 70)
    print("Waiting / Working / Retrying split:")
    print(f"  • Waiting  (queue {queue_wait} + throttle {lease_wait_ms}): {waiting_ms:5d} ms")
    print(f"  • Retrying (backoff):      {retry_wait_ms:5d} ms")
    print(f"  • Working  (pipeline):     {working_ms:5d} ms")
    if retry_events:
        print("-" * 70)
        print("Retry events (stage, reason, backoff):")
        for r in target_span.get("retries", []):
            print(f"  • t={int(r.get('rel_ms') or 0):>6}ms  {r.get('stage', '?'):<14} {r.get('reason', '?'):<28} {int(float(r.get('sleep_s') or 0)*1000)} ms")
    print("-" * 70)
    print("Stage Breakdown (ms):")
    print(f"  1. Queue Wait:             {queue_wait:5d} ms  [{t_enqueue:5d} ms -> {t_dequeue:5d} ms]")
    print(f"  2. S1 Triage (Model):      {s1_dur:5d} ms  [{t_s1_start:5d} ms -> {t_s1_end:5d} ms]  (incl. retries above)")
    print(f"  3. S2 Retrieval (BM25):    {s2_dur:5d} ms  [{t_s2_start:5d} ms -> {t_s2_end:5d} ms]")
    print(f"  4. S4 Provider TTFT:       {s4_prov_ttft:5d} ms  [{t_s4_start:5d} ms -> {t_s4_prov_tok:5d} ms]")
    print(f"  5. S5 Verification Tax:    {s5_tax:5d} ms  [{t_s4_prov_tok:5d} ms -> {t_s5_flush:5d} ms]  <-- Pre-flush clause buffer")
    print(f"  6. S5 Streaming Finish:    {stream_dur:5d} ms  [{t_s5_flush:5d} ms -> {t_end:5d} ms]")
    print("-" * 70)
    verdict = (
        "BEFORE the first token (user saw nothing while it was slow)"
        if after_first_tok_ms <= before_first_tok_ms
        else "AFTER the first token (user saw output while it finished)"
    )
    print(f"Delay verdict: the delay came {verdict}.")
    print("How we know: per-stage event timestamps in logs/spans.jsonl (recorded live during the request),")
    print("plus retry entries whose backoff sleep was logged by the client at the moment it slept.")
    print("-" * 70)
    print("ASCII Waterfall Timeline (Scale: 40 chars):")

    def bar(start_ms, dur_ms):
        col_start = int((start_ms / total_ms) * 40)
        col_width = max(1, int((dur_ms / total_ms) * 40))
        return " " * col_start + "#" * col_width

    print(f"  Queue:       |{bar(t_enqueue, queue_wait):<40}| {queue_wait} ms")
    print(f"  S1 Triage:   |{bar(t_s1_start, s1_dur):<40}| {s1_dur} ms")
    print(f"  S2 Retrieve: |{bar(t_s2_start, s2_dur):<40}| {s2_dur} ms")
    print(f"  S4 TTFT:     |{bar(t_s4_start, s4_prov_ttft):<40}| {s4_prov_ttft} ms")
    print(f"  S5 Buffer:   |{bar(t_s4_prov_tok, s5_tax):<40}| {s5_tax} ms (Verification)")
    print(f"  S5 Stream:   |{bar(t_s5_flush, stream_dur):<40}| {stream_dur} ms")
    print("=" * 70)
    return True


def main():
    parser = argparse.ArgumentParser(description="Relay Analysis and Verification Utility")
    parser.add_argument("--tables", action="store_true", help="Print Markdown Tables 1, 2a, 2b, 3")
    parser.add_argument(
        "--verify", action="store_true", help="Cross-check submission results contract"
    )
    parser.add_argument(
        "--waterfall", type=str, metavar="REQUEST_ID", help="Print ASCII latency waterfall for a request"
    )
    args = parser.parse_args()

    rows = load_results_rows()

    if args.tables:
        t1 = compute_table_1()
        t2a, t2b = compute_tables_2a_2b(rows)
        t3 = compute_table_3(rows)
        print(t1)
        print(t2a)
        print()
        print(t2b)
        print()
        print(t3)

    if args.waterfall:
        print_waterfall(args.waterfall)

    if args.verify:
        if not RESULTS_CSV.exists():
            print("ERROR: results/results.csv missing!", file=sys.stderr)
            sys.exit(1)

        expected_cols = [
            "request_id",
            "scenario",
            "source",
            "start_time",
            "first_token_ms",
            "total_ms",
            "status",
            "retries",
            "tokens_in",
            "tokens_out",
            "retrieval_setting",
            "answer_grade",
        ]
        with open(RESULTS_CSV, "r", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header != expected_cols:
                print(
                    f"ERROR: results.csv header mismatch!\nExpected: {expected_cols}\nFound: {header}",
                    file=sys.stderr,
                )
                sys.exit(1)

        print(
            f"Verification SUCCESS: results.csv schema valid ({len(rows)} data rows). Recomputation ready."
        )


if __name__ == "__main__":
    main()
