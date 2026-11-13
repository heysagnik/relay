# AGENT.md — "The Relay" Project Runbook

> **Audience:** any coding agent (or engineer) picking this project up cold. Read this file top-to-bottom before writing any code.
> **Authority:** the assignment brief (`The_Relay_Assignment_Brief.pdf`). This file embeds the brief's requirements. If the two ever disagree, **the brief wins** — then patch this file.
> **Golden rule:** every number in the submission must trace to a measurement stored in `results/results.csv`, `logs/calibration.json`, or `logs/spans.jsonl`. No number without a row behind it. "Not measured" is an acceptable answer; an invented number is a failed submission.

---

## 1. Purpose

Build a small, working RAG assistant — "the relay" — that answers engineers' questions about **exactly three pinned arXiv papers**, using the **real Groq free tier** for model calls, and survives bursty load, provider rate limits, timeouts, and slow calls.

The company wants four things that cannot all hold: **fast replies, good replies, a small bill, and nobody turned away.** This project is not about maximizing any one of them — it is about *choosing deliberate trade-offs, backing each choice with measurements, and stating what each choice costs*. Reviewers recompute every table number from the raw file, and a live conversation follows where you must defend and change the system. **Never sacrifice correctness for speed**: a wrong or fabricated answer is the worst failure mode.

What is being graded:
1. Measurements that actually drive the design decisions.
2. Honest accounting of trade-off costs (who is delayed, who is dropped, who gets a weaker answer).
3. The distinctly-human choices — see §12 (Agent conduct) for what the agent may not fabricate.

---

## 2. Deliverables (what "done" means)

| # | Deliverable | Where it lives | Hard requirements |
|---|---|---|---|
| D1 | Working pipeline code | `src/relay/` | $\ge 4$ steps, $\ge 1$ tool call, $\ge 1$ branch AND $\ge 1$ loop; retrieval step over the 3 papers; every answer cites its source or refuses honestly |
| D2 | Stand-in model service | `src/relay/standin_server.py` | Can be slow, fail, refuse-at-limit, degrade under load, stream piece-by-piece; calibrated from real measurements |
| D3 | Raw results file | `results/results.csv` | Exact schema in §5.1; every table number recomputable from it |
| D4 | Write-up | `writeup/relay_writeup.pdf` (or `.md`+PDF) | $\le 2$ pages, fixed headings in fixed order (§5.3), all 3 tables copied in, fully filled |
| D5 | Conversation history | `writeup/conversation_history.txt` | Real, unedited export of the human–assistant chats that built this (prompts only, if the tool cannot export) |
| D6 | Decision log | `DECISIONS.md` | Every trade-off with the measurement that forced it and its measured cost — feeds the write-up |

Also submitted: the code repo and the raw per-run files (`results/runs/<timestamp>/`) — "We will look at them."

---

## 3. Hard constraints (never violate)

1. **Deadline:** submit within 4 days of receiving the brief. Budget ~6–8 focused hours total.
2. **Write-up:** maximum 2 pages of writing (code, raw results, and chat history do not count).
3. **Corpus:** exactly these three papers, fetched yourself at these pinned versions — no other knowledge source for answers:
   - *Attention Is All You Need* — arXiv 1706.03762 **v7**
   - *Moshi: a speech-text foundation model for real-time dialogue* — arXiv 2410.00037 **v1**
   - *Dynamic Chunking for End-to-End Hierarchical Sequence Modeling* — arXiv 2507.07955 **v2**
4. **Provider:** real Groq free tier with the human's own free account and key. **Never print, log, or commit the key.** No extra accounts, no limit workarounds — working *inside* the limits is the point.
5. **Shared quota:** everything (calibration, eval, load tests) burns one daily quota per model. Plan requests (§10.5); running out is part of the problem — the design must survive it.
6. **Answers:** cite where in the papers they came from. If the papers don't say, the assistant says so instead of guessing.
7. **Results tables:** every cell filled or explicitly "not measured". Milliseconds for time, one decimal for percentages.
8. **Stack:** Python 3.11+ (locked decision — rationale in §6).

---

## 4. The four goals and the declared stance

The brief: "It cannot have all four." Decide explicitly what wins. Record in `DECISIONS.md` as decision D-001:

**Recommended stance (tune after Phase 1 measurements, then lock):**
- **Priority order:** correct $>$ fast $>$ nobody-turned-away $>$ cheap (the bill is $\$0$ on free tier anyway — cheap becomes *quota longevity*).
- **"Fast" defined as an SLO** (decision D-002, tune then lock): TTFT $p_{50} \le 2500\text{ ms}$ and total $p_{95} \le 12\text{ s}$ under *steady* load; under burst/overload, minimize give-ups rather than chase $p_{95}$.
- **Correctness is never traded:** refusal is always allowed; fabricated citations are a hard failure (hence the clause-buffered programmatic citation verifier, §8.1 S4/S5).
- **Who pays when limits bite:** newcomers are delayed or told the truth; nobody gets a silently wrong answer (degradation ladder in §8.4).

---

## 5. Data contracts (do not deviate by a character)

### 5.1 `results/results.csv` — one row per request

Columns, in order:

```
request_id, scenario, source, start_time, first_token_ms, total_ms, status,
retries, tokens_in, tokens_out, retrieval_setting, answer_grade
```

| Column | Values / format |
|---|---|
| `request_id` | unique per run, e.g. `steady-0007` |
| `scenario` | `steady` / `burst` / `overload` / `rpm_halved` / `budget_exhausted` (§8.8) |
| `source` | `real` or `stand-in` — per request, not per run (mixed runs are fine) |
| `start_time` | ISO-8601 with timezone, request arrival (enqueue) time |
| `first_token_ms` | arrival $\rightarrow$ **first user-visible verified answer token** (queue wait + first-clause verification included, §8.2). Blank if no token ever arrived. |
| `total_ms` | arrival $\rightarrow$ final answer delivered / given up / failed |
| `status` | `ok` / `rate_limited` / `timeout` / `error` |
| `retries` | integer count of model-call retries consumed by this request |
| `tokens_in` / `tokens_out` | summed across all model calls in the request, from provider `usage` fields |
| `retrieval_setting` | `A` / `B` / `C` / `A-degraded` (§8.7) |
| `answer_grade` | `correct` / `wrong` / `refused_correctly` / `refused_wrongly` (blank if status $\neq$ ok) |

### 5.2 The three tables (copy into write-up, fully filled)

**Table 1 — Limits found in the real service (per model: 8B triage vs. 70B draft).** Rows: requests/min, tokens/min, requests/day, tokens/day, requests in flight at once, what it returns when a limit is hit, time until it recovers. Columns: value found / how you found it / confidence (high, medium, low).

**Table 2a — Latency, one row per scenario** (at least: steady, burst, sustained overload, per-minute limit halved, daily budget exhausted). Columns: scenario / real or stand-in / requests sent / duration (s) / offered rate (per min) / first-token $p_{50}$ / $p_{95}$ / $p_{99}$ (ms) / total-time $p_{50}$ / $p_{95}$ / $p_{99}$ (ms).

**Table 2b — Outcomes, same rows.** Columns: scenario / succeeded % / rate-limited % / timed-out-or-given-up % / retries per request / peak queue length / tokens per answer / cost per 1,000 answers.

**Table 3 — Retrieval settings (at least three).** Columns: setting (piece size, pieces fetched, re-rank yes/no) / questions asked / correct-and-cited % / wrong % / correctly-refused % / tokens per answer / first-token $p_{50}$ / $p_{95}$ (ms) / total-time $p_{50}$ / $p_{95}$ (ms).

Cost per 1,000 answers: $\text{tokens\_per\_answer} \times 1000 \times \text{Groq's published per-token price}$ for the models used (record the price and the date you read it). Free tier actual spend is $\$0$ — report both lines; that honesty is worth marks.

### 5.3 Write-up headings — exact titles, exact order (a missing heading = incomplete page)

```
1 What I built and why
2 The real service and my stand-in
3 Results
4 Where the time goes
5 When a limit is hit
Retrieval: quality versus cost
How this library will mislead a naive retriever
6 Retries
7 What the person sees
8 What I'm unsure about, what I left out, what I learned
Your feedback on Vaani
How I worked with the assistant
```

---

## 6. Technology stack (locked; changing any of these requires a DECISIONS.md entry)

- **Python 3.11+**, `asyncio` throughout. Rationale: the bottleneck is the model service and the quota — never our CPU — so Go/C++ buys nothing here, and Python keeps the 6–8 h budget realistic.
- `httpx` (async HTTP + SSE streaming) — Groq client and stand-in client share one transport.
- `FastAPI` + `uvicorn` — the stand-in service.
- `PyMuPDF` (`fitz`) — PDF $\rightarrow$ text with page numbers.
- `rank-bm25` — retrieval (zero provider tokens). Optional rerank: `sentence-transformers` `ms-marco-MiniLM-L-6-v2` on CPU.
- `PyYAML`, stdlib `statistics`, `json` + `asyncio.Lock` (atomic in-memory + disk-flushed budget ledger). No other frameworks.
- **Models** (verify current availability in Phase 0/1; names change): triage = small-instant class (`llama-3.1-8b-instant`); draft = 70B class (`llama-3.3-70b-versatile`). See Appendix D for planning math.

---

## 7. Repository layout

```
relay/
├── AGENT.md                     # this file
├── DECISIONS.md                 # timestamped trade-off log (D-001, D-002, ...)
├── .env                         # GROQ_API_KEY=...  (gitignored, never printed)
├── .env.example                 # "GROQ_API_KEY=" only
├── requirements.txt
├── papers/                      # 3 PDFs at pinned versions + extracted .txt with page markers
├── data/
│   ├── chunks/                  # per setting: A/, B/, C/ (chunks + metadata.json)
│   ├── questions.yaml           # ≥15 eval questions + gold + rubric (§8.9)
│   └── budget.json              # persistent per-model daily ledger (§8.4)
├── src/relay/
│   ├── config.py                # all knobs (SLO, patience, concurrency, models, retrieval settings)
│   ├── groq_client.py           # real client: streaming, 429 capture, header sync, retries, usage
│   ├── standin_server.py        # stand-in: OpenAI-compatible, calibrated knobs (§8.6)
│   ├── chunker.py               # PDF → chunks with {paper, arxiv_id, version, section, page}
│   ├── retrieval.py             # BM25 + optional target_paper filter + rerank; settings A/B/C
│   ├── orchestrator.py          # the relay: queue, stages, branch, clause-verified stream loop, spans
│   ├── budget.py                # per-model ledger + header hydration + degradation ladder
│   ├── loadgen.py               # open-loop load runner, scenarios.yaml
│   ├── grade.py                 # eval grading (auto checks + human review pass)
│   └── analyze.py               # recomputes Tables 1–3 from results.csv; verify mode
├── scenarios.yaml               # the ≥5 load scenarios (§8.8)
├── results/
│   ├── results.csv              # THE raw file (D3)
│   └── runs/<timestamp>/        # per-run copies: results rows, spans.jsonl, run config
├── logs/
│   ├── calibration.json         # Phase 1 raw measurements (Table 1 evidence)
│   └── spans.jsonl              # per-request span traces (§8.2)
└── writeup/
    ├── relay_writeup.pdf        # ≤2 pages, headings §5.3
    ├── conversation_history.txt # unedited export (D5)
    └── vaani_feedback.txt       # human's honest feedback on vaani.dev
```

---

## 8. Architecture

### 8.1 The relay pipeline (per request)

```
 arrival ──▶ [queue] ──▶ S1 Triage ──▶ S2 Retrieve ──▶ score gate ──▶ S4 Draft ──▶ S5 Verify ──▶ user stream
                │          (8B LLM,      (TOOL:         (branch)      (70B LLM,     (TOOL:        (clause-by-
                │           JSON out)     local BM25 +    │            provider      clause &      clause flush)
                │                         paper filter)   │            SSE stream)   citation        │
                │                                         │                          check)          │ unverified cite
                │                                   score < τ or                       │             │ or bad quote
                │                                   out-of-scope                       │             │ before flush:
                │                                         │                   fails on 1st pass:     │ abort stream,
                │                                         ▼                   loop ONCE (expand      │ re-retrieve,
                │                                   S3 Refuse ◀────────────── query, redraft); ──────┘ redraft; fail
                │                                   (honest template)         2nd failure ⇒ refuse     again ⇒ refuse
                └─ queue wait > PATIENCE ⇒ status=timeout ("given up"), person is told (§8.4 messages)
```

Stage table (this is how the brief's "$\ge 4$ steps, $\ge 1$ tool call, $\ge 1$ branch/loop" is satisfied — point reviewers here):

| Stage | Kind | What it does | Why |
|---|---|---|---|
| S1 Triage | LLM call (`8b-instant`) | Output structured JSON: `{"classification": "answerable\|out_of_scope\|ambiguous", "target_paper": "AISNY\|Moshi\|DynChunk\|ALL", "queries": [...]}` | Cheap insurance: unanswerable questions exit early; `target_paper` prevents vocabulary collision across papers (Appendix C #1) |
| S2 Retrieve | **Tool call** (local) | BM25 over chunk index (filtered/boosted by `target_paper` if specified); returns passages + `{paper, section, page}` + scores | Zero provider tokens; all TPM budget goes to S1/S4 |
| S3 Gate + Refuse | **Branch** | If best score $< \tau$ (default $6.0$, tune in Phase 3) or triage = `out_of_scope` / `ambiguous` $\rightarrow$ honest refusal with what was tried | "If the papers do not say, say so instead of guessing" |
| S4 Draft | LLM call (`70b-versatile`, streamed) | Stream tokens from provider into a **clause buffer** (splits on sentence boundaries or closing citation brackets `[Paper §X, p.Y]`) | Decouples provider streaming from user-visible tokens so we never emit an unverified citation to the user |
| S5 Verify | **Tool call** (programmatic, inline) | As each clause completes in S4, verify: (a) any `[Paper §X, p.Y]` tag matches a retrieved chunk in top-$k$, and (b) any quoted span fuzzy-matches ($\ge 90\%$ similarity) a retrieved chunk. Verified clauses flush immediately to the user | Preserves streaming TTFT (first verified clause flushes as soon as its citation closes) while guaranteeing zero hallucinated citations reach the user |
| Loop | **Loop** (max 1 iteration) | If the first clause/citation in S5 fails verification before any user flush (or if no valid citation appears in the draft), abort stream, re-retrieve (top-$k \times 2$, drop paper filter, use alternate query), and redraft once; second failure $\rightarrow$ S3 Refuse | Second chances recover from retrieval misses, while pre-flush buffering ensures we never "un-send" tokens already shown to the user |

### 8.2 Timing model and span instrumentation (build this on day one, not bolted on later)

- **TTFT (`first_token_ms`)** = arrival (`enqueue`) $\rightarrow$ **first user-visible verified token** (`s5_first_flush`). Queue wait AND first-clause citation verification count, because the person sees nothing until the first verified clause is flushed. We also log `s4_provider_first_token` in `spans.jsonl` so we can quantify the exact latency cost of pre-flush verification (typically $150\text{–}400\text{ ms}$) in the write-up. Generation time = $\text{total} - \text{TTFT}$. Record this in `DECISIONS.md` as D-003.
- Every request writes a span record to `logs/spans.jsonl`:

```json
{"request_id":"steady-0007","scenario":"steady","source":"real",
 "events":[["enqueue",0],["dequeue",812],["s1_start",815],["s1_end",1320],
           ["s2_start",1322],["s2_end",1365],["s4_start",1367],
           ["s4_provider_first_token",1890],["s5_first_flush",2210],
           ["s4_end",4180],["s5_end",4201]],
 "retries":[{"stage":"s4","attempt":2,"reason":"429","delay_ms":2000}],
 "model_calls":[{"stage":"s1","model":"llama-3.1-8b-instant","tokens_in":380,"tokens_out":65}],
 "queue_len_at_dequeue":3}
```

- The waterfall of one slow request printed from this file is the entire evidence for write-up heading **4 Where the time goes** (waiting = `enqueue`$\rightarrow$`dequeue`; triage+retrieve = `s1_start`$\rightarrow$`s2_end`; provider TTFT = `s4_start`$\rightarrow$`s4_provider_first_token`; verification buffer cost = `s4_provider_first_token`$\rightarrow$`s5_first_flush`; streaming generation = `s5_first_flush`$\rightarrow$`s5_end`).
- **PATIENCE_MS = 15000** default (decision D-004, tune after Phase 5): "people give up sooner than you expect."

### 8.3 Concurrency, queue, admission control

- One async orchestrator; `asyncio.Semaphore(CONCURRENCY)` for model calls (start at 4; tune from measured behavior).
- Bounded queue (capacity 200). Queue wait $>$ PATIENCE $\rightarrow$ `status=timeout`, person gets the give-up message (§8.4).
- **Admission control at enqueue:** if the ledger (§8.4) is above soft-TPM/RPD or the queue is above `Q_MAX` (default 40), the request is served in degraded mode immediately instead of queueing — this is "who gets a weaker answer," by design, not by accident.

### 8.4 Budget guard and the degradation ladder (the heart of "when a limit is hit")

`data/budget.json` — persistent **per-model** ledger, guarded in-process by an `asyncio.Lock()` and flushed atomically to disk via temp-file rename (`budget.json.tmp` $\rightarrow$ `budget.json`) so concurrent tasks never corrupt it:

```json
{
  "date": "2026-11-10",
  "models": {
    "llama-3.1-8b-instant": {"rpd_used": 0, "tpd_used": 0, "rpm_window": [], "tpm_window": [], "hdr_rem_req": null, "hdr_rem_tok": null, "hdr_reset_tok_s": 0.0},
    "llama-3.3-70b-versatile": {"rpd_used": 0, "tpd_used": 0, "rpm_window": [], "tpm_window": [], "hdr_rem_req": null, "hdr_rem_tok": null, "hdr_reset_tok_s": 0.0}
  }
}
```

**Header Hydration Rule:** Groq enforces limits **per model ID**, not globally across the organization. On every completed response (200 or 429), `groq_client.py` parses `x-ratelimit-remaining-requests` (RPD remaining), `x-ratelimit-remaining-tokens` (TPM remaining), and `x-ratelimit-reset-tokens` and reconciles the model's ledger entry so local sliding windows never drift from Groq's actual counters.

| Condition (checked before each stage on target model) | Action | Who pays |
|---|---|---|
| RPM $\ge 80\%$ of window | Space new calls using `hdr_reset_tok_s` / window age to stay under; no retries this window | Newcomers: delayed (queue grows) |
| 70B TPM $\ge 80\%$ of window (or `hdr_rem_tok` $< 3500$) | Degrade retrieval (`A-degraded`): top-$k$ $6\rightarrow 2$, trim context (~50% fewer `tokens_in`); if 70B TPM $< 1500$, route S4 draft to `8b-instant` with strict citation check | Users: weaker/terser answer (fewer cited passages), still verified & correct |
| 70B RPD or TPD $\ge 90\%$ of day | Only refusal-path + `A-degraded` on `8b-instant` allowed; complex cross-paper queries get "busy today, daily 70B budget low" | Users: delayed/deferred or served by fallback model |
| RPD or TPD $= 100\%$ (the 3pm moment) | Every arrival gets the budget-gone message (below); nothing queues, nothing errors silently | Everyone: told the truth, nobody served |
| Stand-in in use | Same ladder, driven by the stand-in's own per-model limits and simulated `x-ratelimit-*` headers | — |

**The 3pm reply (write-up heading 7) — draft, human finalizes:**

> "I can't answer this one right now. Our model provider's free-tier daily quota ($N$ requests / $M$ tokens per day on `llama-3.3-70b-versatile`) ran out at 14:5X. Nothing is queued and retrying now would just fail — that's deliberate, not neglect. Your question is safe with me: ask again after the limit resets at 00:00 UTC (in Xh Ym). Sorry — this one is on our capacity planning, not on the papers."

No spinner, no fake answer, no uncited guess. Include time-to-reset and the honest cause.

### 8.5 Groq client (the real service)

- `httpx.AsyncClient`, SSE streaming always (provider TTFT is only measurable when streaming).
- **Capture every 429 verbatim** — full body + all headers — into `logs/calibration.json`. Groq sends `retry-after` (seconds), `x-ratelimit-limit-requests` (RPD), `x-ratelimit-limit-tokens` (TPM), `x-ratelimit-remaining-requests`, `x-ratelimit-remaining-tokens`, `x-ratelimit-reset-requests`, `x-ratelimit-reset-tokens`; **verify what actually arrives** — that captured artifact *is* Table 1's evidence column.
- Token accounting from the `usage` field of the final streamed chunk; `tokens_in`/`tokens_out` accumulate per request into `results.csv`.
- Timeout ladder (decision D-005, tune in Phase 5): connect $5\text{ s}$ / first-token $12\text{ s}$ / inter-token gap $10\text{ s}$ / total $45\text{ s}$.
- **Retry policy** in §11. Never retry content refusals or non-429 4xx.
- **RPD/TPD probe rule: never burn the day's quota to force a daily-limit 429.** Read the limits page in the console + headers (`x-ratelimit-limit-requests` reports RPD directly on every 200 response!); record confidence honestly. If you *naturally* exhaust RPD/TPD during load runs, capture it (high confidence, and a good story).

### 8.6 Stand-in model service ("the double")

FastAPI app exposing OpenAI-compatible `POST /v1/chat/completions` (stream + non-stream) plus `GET /__config` (live knobs) and `GET /__stats`. The pipeline talks to it by swapping `base_url` only — identical code path, including emitting realistic `x-ratelimit-*` headers so `budget.py` behaves identically against both services.

| Brief-required behavior | Knob | Default |
|---|---|---|
| Be slow | `ttft_dist`: lognormal fitted to real TTFT samples per model; `itt_dist`: inter-token times | fitted (Phase 4) |
| Fail | `error_rate` (immediate 500 / connection reset) | $0.05$ |
| Refuse because a limit was reached | Per-model `rpm_limit`, `tpm_limit`, `rpd_limit`, `tpd_limit` $\rightarrow$ Groq-shaped 429 body + headers, honoring `recovery_s` | from Table 1 |
| Become slower under load | `degrade_ms_per_inflight` added per in-flight request beyond a soft-concurrency knee | fitted from burst run, or stated assumption |
| Succeed slowly instead of failing fast | `slow_success_rate` $p \rightarrow \times 8\text{–}20$ latency multiplier, still 200 | $0.05$ |
| Reply a piece at a time | SSE chunks emitted per `itt_dist` $\times$ chunk size, including valid citations from prompt context so S5 passes | on |

Calibration procedure (Phase 4): fit `ttft_dist`/`itt_dist` to `logs/calibration.json` per model (report the fit, e.g. KS distance or quantile match); set limits from Table 1; sanity-check that a stand-in steady run lands within ~20% of the real steady run's $p_{50}$ — else refit. **Write down where it is least like the real thing** (write-up heading 2): candidates — no multi-tenant provider queue fairness, no hardware cold starts, no token-level jitter correlation, single local loopback region, deterministic token estimation ($\approx \text{chars}/4$).

### 8.7 Retrieval subsystem

- PDFs $\rightarrow$ text via PyMuPDF, keeping page numbers; strip running headers/footers heuristically; tables flatten to text lines (record as known extraction weakness — it feeds Appendix C).
- Chunks: sentence-aware, ~setting-sized, 15% overlap; metadata `{paper, arxiv_id, version, section, page, chunk_id}`; citations render as `[AISNY §3.2, p.4]` / `[Moshi §5, p.9]` / `[DynChunk §4.1, p.6]`.
- Index: BM25 over lowercased text, with optional `target_paper` metadata filter/boost passed from S1 Triage. Rerank (setting C only): cross-encoder `ms-marco-MiniLM-L-6-v2`, CPU.

**The three settings (rows of Table 3):**

| Setting | Chunk size (tokens) | Pieces fetched | Re-rank | Point in the space |
|---|---|---|---|---|
| A | ~350 (overlap ~50) | top-6 | no | baseline: medium-few |
| B | ~150 | top-12 | no | small-many: precision context, more tokens |
| C | ~350 | top-6 $\rightarrow$ rerank $\rightarrow$ top-3 | yes | medium-few + precision (~50% fewer draft prompt tokens than A) |

Exact sizes adjustable $\pm 30\%$ while building; keep the three points distinct and log the final values.

**When the limit is close** (write-up, *Retrieval* heading): degrade A $\rightarrow$ `A-degraded` (top-3 fetched, context trimmed to best 2 chunks, ~50% fewer `tokens_in`). Measure the quality cost with a mini re-run (8 questions) on `A-degraded` so the trade-off has a number, not a shrug.

### 8.8 Load runner and the $\ge 5$ scenarios

`loadgen.py` is **open-loop**: a timer wheel fires requests at the offered rate regardless of completions — otherwise you never observe real queueing. Verify offered vs actually-sent counts per run. Questions are sampled from the full eval set (including unanswerables — realistic mix).

**Quota-Aware Steady Rate (Critical Fix):** Because a full pipeline call consumes ~2.5–3.5K tokens on the 70B model and Groq's free 70B TPM limit is ~12K TPM (and 100K TPD), firing 30 req/min on the real API is actually a **10x TPM overload** that will hit 429s in 15 seconds and burn the entire day's 100K TPD in 3 minutes. Therefore, we define `steady` as **75% of the binding sustainable rate measured in Phase 1** ($\approx 3\text{ req/min}$ over $300\text{ s} = 15\text{ requests}$ on `real`, plus a matching `stand-in` twin), while running high-RPM stress scenarios on the calibrated `stand-in`. Record this math explicitly in `DECISIONS.md` as D-007.

`scenarios.yaml` (all five scenario categories are mandatory in Tables 2a/2b):

| scenario | offered rate | duration | source | what it shows |
|---|---|---|---|---|
| `steady` | $3\text{/min}$ ($\approx 75\%$ of 70B TPM ceiling; tune from Phase 1) | $300\text{ s}$ ($15\text{ req}$) | **real** (and a **stand-in** twin row for direct comparison) | baseline latency + quality under sustainable load without 429s |
| `burst` | $0 \rightarrow 30\text{/min}$ spike over $30\text{ s}$ ($15\text{ req}$), then $0$ | ~$90\text{ s}$ | **stand-in** (+ a **real** 8-request mini-burst over $16\text{ s}$ to observe real TPM/RPM 429s) | queueing, TTFT inflation, TPM degradation ladder, give-ups |
| `overload` | $4\times$ sustainable ceiling ($12\text{–}16\text{/min}$) | $300\text{ s}$ | **stand-in** (run twice: once without cooldown, once with cooldown — §11) | sustained collapse: retries amplifying load, peak queue, ladder behavior |
| `rpm_halved` | steady rate ($3\text{/min}$) | $300\text{ s}$ | **stand-in** with `rpm_limit` & `tpm_limit` $= \text{measured}/2$ | per-minute limit halved: who is delayed vs served degraded vs dropped |
| `budget_exhausted` | seed ledger to $\text{RPD}-3$ (or $\text{TPD}-8000$), fire $15\text{ req}$ at $6\text{/min}$ | ~$150\text{ s}$ | **stand-in** (or **real** if naturally near end-of-day quota) | the transition from 90% degradation to the 100% 3pm refusal message |

Keep every run's raw rows + spans + config under `results/runs/<timestamp>/`.

### 8.9 Eval and grading

`data/questions.yaml` — $\ge 15$ questions, each: `{id, question, gold_answer_summary, expected_source (paper §/page), grade_rubric}`. Composition: ~4–5 factual per paper (numbers and section-level facts), 1–2 cross-paper comparisons, $\ge 4$ deliberately unanswerable (out-of-corpus, absent-detail, shared-vocabulary trap — see Appendix B).

Grading (write-up heading 3 requires it): run each question through the pipeline per retrieval setting; auto-check answer contains the gold key facts + a citation matching the expected source; then a **human review pass** on every graded answer (the grader field records `auto+human`). Map to the four grades; `refused_wrongly` (papers DID say, assistant refused) counts against quality.

### 8.10 Analysis and verification (the submission gate)

`analyze.py --tables` recomputes Tables 1–3 from `results/results.csv` (+ `logs/calibration.json` for Table 1) and prints them as markdown with the brief's units (ms, 1-decimal %). `analyze.py --verify` cross-checks every number that appears in the write-up tables against the recomputation and exits non-zero on any mismatch. **Run `--verify` before every submission.** Reviewers recompute from the raw file; so must we, automatically, every time.

---

## 9. Phased runbook (order matters; time and request budgets included)

| Phase | Time | Do | Token / Request budget (real) | Exit criteria |
|---|---|---|---|---|
| **0 Setup** | 30 min | Repo + env + `.env`; fetch the 3 PDFs at pinned versions (Appendix A) and verify the version string; extract text with page markers; confirm both model IDs exist on the account | $0$ | 3 txt files with page markers; `python -c "import fitz"` works |
| **1 Calibrate** | 45 min | ~20 real requests ($10\times$ 8B, $10\times$ 70B), spaced under RPM/TPM: record TTFT, inter-token times, totals, usage, and `x-ratelimit-*` headers. One deliberate RPM/TPM probe on 8B (or short prompt on 70B): fire until first 429 ($\le 35$ short requests), capture body+headers verbatim, probe every $5\text{ s}$ until 200 returns. Fill Table 1 draft + `logs/calibration.json`. **Do not force an RPD/TPD hit.** | $\le 55$ calls ($\approx 12\text{K}$ 70B tokens) | Table 1 draft with methods + confidences; `calibration.json` exists |
| **2 Pipeline** | 90 min | Build orchestrator (with clause-buffered S4/S5 stream verification), paper-filtered retrieval, per-model header-synced ledger, spans — with span logging from the first commit. Unit-test against **recorded fixtures**, never against live quota | $0$ | One end-to-end fixture request produces a cited answer; `spans.jsonl` populated |
| **3 Eval + retrieval sweep** | 45 min | Finalize `questions.yaml` (15 questions; human verifies gold); run eval on A, B, C (paced at $2.5\text{ req/min}$ to avoid 429 noise during quality measurement) + 8-question mini-run on `A-degraded`; grade; draft Table 3 | $53$ pipeline runs ($\approx 38$ answerable reaches S4 $\approx 110\text{K}$ tokens; if 70B TPD is $100\text{K}$, run A & C on real 70B and B on stand-in or split across UTC midnight reset — log in `DECISIONS.md`) | Table 3 draft; $\ge 15$ questions graded `auto+human` |
| **4 Stand-in** | 45 min | Build stand-in; fit knobs from `calibration.json`; sanity run (stand-in steady $\approx$ real steady within ~20% $p_{50}$) | $0$ (uses Phase 1/5 data) | Fitted params + written "least like the real thing" note |
| **5 Load scenarios** | 45 min | Run all 5 scenarios (`steady` + mini-`burst` on real; all 5 full scenarios on calibrated stand-in); keep raw; draft Tables 2a/2b | `steady` real ($15$ req) + mini-`burst` real ($8$ req) $\approx 50\text{K}$ 70B tokens | `results.csv` rows for all scenarios; tables draft |
| **6 Slow-request trace** | 30 min | Pick the most interesting slow request; print its waterfall (including `enqueue`$\rightarrow$`dequeue`, `s4_provider_first_token`, and `s5_first_flush`); write the before/after-first-token split | $0$ | The heading-4 evidence exists as a figure/table |
| **7 Assemble** | 60 min | `analyze.py --verify` green $\rightarrow$ write-up (2 pages, headings §5.3) $\rightarrow$ `DECISIONS.md` final $\rightarrow$ export chat history **unedited** $\rightarrow$ human writes Vaani feedback $\rightarrow$ final checklist §13 | $0$ | All §13 checkboxes pass |

If quota runs out mid-project: the ledger switches everything to stand-in, the incident is recorded in `DECISIONS.md`, and the write-up's heading 8 gains a real story ("what breaks if it's wrong"). That is the assignment working as intended, not a failure.

---

## 10. Measurement plan

- **Latency spread:** per model — TTFT and total $p_{50}/p_{95}/p_{99}$ + a small summary, from $\ge 10\text{–}20$ samples each. State $n$ in the write-up.
- **Limits discovery (Table 1 "how found"):**
  - **RPM / TPM:** read directly from `x-ratelimit-limit-tokens` header + deliberate burst probe until observed 429 (high confidence).
  - **RPD / TPD:** read directly from `x-ratelimit-limit-requests` header + Groq console limits page (high/medium confidence; capture naturally if hit).
  - **In-flight concurrency:** fire $K$ parallel short requests, count 200s vs 429s, check if limit is concurrency-bound or purely RPM/TPM-bound (confidence: medium).
  - **What-it-returns + recovery:** from the captured 429 JSON body + `retry-after` / `x-ratelimit-reset-*` headers + re-probe cadence.
- **The unguessable number** (write-up heading 3 requires one; pick the best *measured* one):
  - Candidates: (1) the verification buffer tax (`s5_first_flush` $-$ `s4_provider_first_token` in ms) required to guarantee zero hallucinated citations; (2) retry amplification factor under overload without cooldown (e.g. $1.4\times$); (3) share of total latency that sits **before** the first token under burst (queueing + TPM pacing dominates, e.g. ~75%).
- **Cost per 1,000 answers:** from $\text{tokens/answer} \times \text{current published Groq prices}$ (record price + date) AND the free-tier $\$0$ line.
- **Daily quota plan (§3 rule 5):** track phase-by-phase planned vs actual requests AND tokens per model in `data/budget.json`. Keep system prompts byte-identical across calls so prompt caching reduces effective token burn where supported.

---

## 11. Retry policy (and the required "made it worse" case)

**Policy (decision D-006):**
- Retry only: 429, timeout, 5xx/network. Never: content refusals, other 4xx.
- Backoff $0.5\text{ s} \rightarrow 2\text{ s} \rightarrow 4\text{ s}$ with $0\text{–}500\text{ ms}$ uniform jitter; **always honor the `retry-after` or `x-ratelimit-reset-tokens` header if present** (and record whether it was).
- Max 2 retries per model call; all retries must fit inside remaining `PATIENCE_MS` budget (if `elapsed_ms + retry_after_ms > PATIENCE_MS`, fail fast to `timeout` or degrade immediately rather than burning quota on a request the user has already abandoned!).
- **Circuit-breaker cooldown on repeat 429s:** two consecutive 429s on a model $\rightarrow$ trip model cooldown for `retry-after` seconds (incoming requests either degrade to the fallback model or wait in queue without firing doomed HTTP calls). Retrying *into* a rate limit only feeds the fire.

**The "worse" case (measured in the `overload` scenario, two sub-runs on stand-in):** run `overload` once with naive blind retries (ignoring cooldown and remaining patience) and once with the patience-aware cooldown policy. The naive version amplifies offered load when the service is degrading — wasting TPM on timed-out users, inflating $p_{95}$ total, and increasing give-ups. Report the measured delta in write-up heading 6.

---

## 12. Agent conduct (how to work on this project)

1. **Read the brief PDF first**, every time you resume; this file is a map, the brief is the territory.
2. **Phase-gated:** never start a phase before the previous phase's exit criteria pass. Never skip `analyze.py --verify`.
3. **DECISIONS.md protocol:** every non-obvious choice gets an entry: `D-0NN | date | decision | measurement that forced it | measured cost`. Propose with evidence; the human approves or overrules; both are recorded. This log *is* the raw material for write-up heading "How I worked with the assistant."
4. **Human-owned items — never fabricate, only draft:** the Vaani feedback (the human must actually visit https://www.vaani.dev/ and react), the conversation-history export, final ownership of trade-offs, and any "what I overruled" story. Drafting is welcome; inventing the human's opinion is disqualifying.
5. **Secrets:** the key lives in `.env` only. Before any commit: `git grep -i "gsk_"` must return nothing. Never print it in logs, errors, or the write-up.
6. **Quota frugality:** unit tests run on recorded fixtures; live calls are for calibration, eval, and the marked load runs only.
7. **Keep everything raw:** every run's rows, spans, configs, and the 429 bodies. "We will look at them."
8. **Brevity:** the write-up is $\le 2$ pages with 12 headings — tables carry the numbers, prose carries the reasoning. Depth over coverage: "pick the few techniques that matter for your measurements and go deep."

---

## 13. Final pre-submission checklist (definition of done)

- [ ] Pipeline: $\ge 4$ steps, $\ge 1$ tool call, $\ge 1$ branch AND $\ge 1$ loop — verifiable in `orchestrator.py` (§8.1 table), with clause-buffered streaming verification
- [ ] 3 papers at pinned versions in `papers/`; every graded answer carries real citations; refusals happen on unanswerables
- [ ] Table 1 complete with method + confidence per row (for both 8B and 70B models); `logs/calibration.json` holds the raw header & 429 evidence
- [ ] Tables 2a/2b: all 5 scenario rows, real-vs-stand-in marked; raw runs kept in `results/runs/`
- [ ] Table 3: $\ge 3$ settings + the `A-degraded` mini-run; $\ge 15$ questions graded `auto+human`
- [ ] `results.csv` schema-valid; `analyze.py --verify` exits 0 — **every table number recomputes**
- [ ] Heading-4 waterfall figure from `spans.jsonl` for one real slow request, showing queue wait, triage/retrieve, provider TTFT, verification buffer tax, and generation
- [ ] Write-up: $\le 2$ pages, all 12 headings in §5.3 order, tables copied in, "not measured" where true, one unguessable number
- [ ] 3pm budget-gone reply text included (heading 7)
- [ ] `git grep -i "gsk_"` clean; no key anywhere
- [ ] `conversation_history.txt` exported unedited + half-page reflection (3 prompts that mattered, 1 error caught, 1 overrule, what is distinctly the human's)
- [ ] `DECISIONS.md`: every trade-off with its measurement and cost

---

## 14. Do NOT

- Invent, round-to-taste, or "estimate into" any table cell.
- Stream unverified citations to the user and then try to "retract" them if S5 fails.
- Submit or expose the API key; use extra accounts or any limit workaround.
- Clean up, rewrite, or summarize the conversation history — it must be the real one.
- Spend quota on unit tests or debugging loops (fixtures only).
- Force the daily limit by burning 1,000 requests or 100K tokens on dummy loops.
- Let the load generator become closed-loop (it must fire at the offered rate no matter what).
- Fabricate the human's Vaani opinion or overrule history.
- Write more than 2 pages, skip a heading, or leave a table cell silently blank ("not measured" is the honest fill).
- Add techniques for coverage: no vector DBs, no heavy frameworks, no extra "just in case" features that carry no measurement.

---

## Appendix A — Pinned sources

| Paper | arXiv | Version | Fetch URL | Verify |
|---|---|---|---|---|
| Attention Is All You Need | 1706.03762 | v7 | `https://arxiv.org/pdf/1706.03762v7` | "v7" appears on the PDF title page / version history on the abs page |
| Moshi (speech-text foundation model) | 2410.00037 | v1 | `https://arxiv.org/pdf/2410.00037v1` | same |
| Dynamic Chunking | 2507.07955 | v2 | `https://arxiv.org/pdf/2507.07955v2` | same |

Short names for citations: `AISNY`, `Moshi`, `DynChunk`.

## Appendix B — Eval question candidates ($\ge 15$ needed; finalize in Phase 3 with the papers open; the human verifies every gold answer — do not trust these sight-unseen)

**AISNY:** dimension of positional encodings vs $d_{\text{model}}$; number of heads and $d_{\text{model}}$ of the big model; dropout rates and where applied; BLEU of Transformer-big on WMT14 EN-DE; hardware used for training the base model (only if stated — check).
**Moshi:** Mimi's frame rate and codebook structure; how simultaneous turns are modeled (parallel streams); what "Inner Monologue" is; the split of text vs audio streams; anything the paper turns out *not* to state about training compute.
**DynChunk:** what the model emits at chunk boundaries; how segment labels are handled; the ablation result that matters most; training details only if stated.
**Cross-paper:** "Which model across the three papers has the most parameters, and how many?"; "Compare Moshi's latency design with Dynamic Chunking's chunk size in one answer."
**Unanswerable traps (grade as `refused_correctly`):** a question about a technique the papers only mention in related work (pick one actually present while reading); "What was the training cost in dollars?" (not stated); a shared-vocabulary question like "How many attention heads does the encoder use?" that is ambiguous across all three papers (tests clarification/refusal discipline, not memory).

## Appendix C — How this library misleads a naive retriever (write-up needs 3 with example questions)

1. **Vocabulary collision across papers.** All three papers are dense with "attention", "encoder", "heads", "embeddings". A naive BM25 query like "how many attention heads does the encoder use" matches all three; TF luck favors the wrong paper (usually AISNY). *Mitigation in our design:* S1 Triage extracts `target_paper` when a paper is named, and flags un-scoped ambiguous questions as `ambiguous` so S3 refuses/asks for clarification instead of guessing.
2. **Related-work mirage.** Each paper describes baselines and prior systems in prose. A question about such a system retrieves a chunk that *mentions* it, and a naive pipeline answers as if the paper contained the fact. Example: "What latency does Whisper achieve?" — Moshi cites it, doesn't characterize it.
3. **Numbers live in tables, not prose.** PDF extraction flattens tables and can orphan captions from cells; chunk boundaries split a table mid-row. Example: asking which configuration achieved the best boundary-$F_1$ in DynChunk's ablations and citing the wrong row.
4. (Bonus) **Boilerplate dominance.** Every abstract/intro says "we propose" — short generic queries top-hit abstracts with no specifics. Example: "what do they propose" returns three abstracts and zero substance.

## Appendix D — Groq free-tier planning math (VERIFY AT RUNTIME — limits change without notice; the assignment requires you to find them yourself)

Official reference: https://console.groq.com/docs/rate-limits and the account's limits page. Free-plan ballparks (verify against your account's limits page and `x-ratelimit-*` headers in Phase 1):
- `llama-3.1-8b-instant` $\approx 30\text{ RPM} / 14,400\text{ RPD} / 6\text{K TPM} / 500\text{K TPD}$
- `llama-3.3-70b-versatile` $\approx 30\text{ RPM} / 1,000\text{ RPD} / 12\text{K TPM} / 100\text{K TPD}$

Limits are enforced **per model ID**; you hit whichever dimension (RPM, TPM, RPD, TPD) comes first. 429s and 200s carry `x-ratelimit-*` headers — capture and hydrate from them on every call.

**Planning consequence (the binding constraint):**
1. **Per-minute ceiling (TPM):** A full pipeline answer costs roughly triage (~400 in / 70 out on 8B) + draft (~2.5–3.5K in / 250 out on 70B). On a $12\text{K TPM}$ 70B draft model, that is **$\sim 3\text{–}4$ answers per minute sustained** — **TPM, not RPM, is the per-minute wall.** That is why our `steady` scenario is set to $3\text{ req/min}$, not $30\text{ req/min}$.
2. **Daily ceiling (TPD):** A $100\text{K TPD}$ limit on 70B allows only **$\sim 30\text{–}35$ full Setting-A answers per day** before hitting the daily token wall! To fit Phase 1, Phase 3, and Phase 5 inside one day's 70B TPD:
   - S1 Triage + S3 Gate filters out the $\ge 4$ unanswerable questions per eval sweep before they ever touch 70B ($0$ 70B tokens spent on refusals).
   - Setting C (reranked top-3) and `A-degraded` (top-2) cut 70B prompt tokens in half (~1.5K tokens/call).
   - When 70B TPD/TPM crosses the 80% threshold, the degradation ladder (§8.4) automatically trims context or shifts draft generation to `llama-3.1-8b-instant` ($500\text{K TPD}$), preserving real-API operation without failing requests.

## Appendix E — Glossary

- **TTFT (`first_token_ms`)** — time to first user-visible verified token, from request arrival (§8.2).
- **Provider TTFT (`s4_provider_first_token`)** — time when the first raw SSE token arrives from Groq/stand-in before clause verification.
- **$p_{50}/p_{95}/p_{99}$** — median / 95th / 99th percentile.
- **Offered rate** — requests/min the generator fires, independent of completions (open-loop).
- **RPM / TPM / RPD / TPD** — provider limits per model: requests/minute, tokens/minute, requests/day, tokens/day.
- **Stand-in** — locally controlled fake model service, calibrated from real measurements.
- **Degradation ladder** — §8.4's ordered responses as limits approach. **Give-up** — a request abandoned because the person's patience expired (`status=timeout`).
- **Span** — one timestamped event in a request's trace; the waterfall is the ordered list of spans.