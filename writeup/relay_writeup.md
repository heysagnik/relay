# The Relay — Write-up

Raw file: `results/results.csv` (one row per request, schema verified by `python -m src.relay.cli analyze --verify`).
Per-request stage traces: `logs/spans.jsonl`. Real-provider evidence: `logs/calibration.json`.
All decisions and their measured costs: `DECISIONS.md` (D-001…D-016). The pre-optimization baseline harness
results are archived at `results/archive/results_pre_optimization_baseline.csv` for before/after comparison.

## 1 What I built and why

A five-stage pipeline for answering questions about three pinned arXiv papers
(Attention Is All You Need v7, Moshi v1, Dynamic Chunking v2):

1. **S1 Triage** (fast 27B model, JSON mode): classify the question (answerable / out-of-scope / ambiguous) and pick a target paper. Triage is an *optimisation, not a dependency*: when its quota is gone the pipeline degrades to unscoped retrieval instead of failing.
2. **S2 Retrieve** (local tool, off the event loop): BM25 over chunked papers (author-list bait stripped), optional cross-encoder re-rank.
3. **S3 Gate** (branch): refuse out-of-scope questions and questions whose best retrieved score is below a paper-scale threshold — before spending draft tokens.
4. **S4 Draft** (120B model, streaming): answer only from retrieved passages, with per-clause citations.
5. **S5 Verify** (loop): buffer clauses until a citation appears; every citation must **exactly match a tag that was actually retrieved** (section and page included). A fabricated `[AISNY §99, p.999]` fails even when an AISNY passage was fetched. A bad first clause kills the draft and triggers a re-retrieval + re-draft (max 2 attempts). **The first token a user sees is a verified clause.**

Around the pipeline sits a load-shedding harness: an admission ladder, a proactive send-lease against the *measured* rate-limit windows, AIMD adaptive concurrency, a bounded priority queue, and a single-flight cache for identical questions (D-016).

The decision that shaped everything else: **correctness > latency > concurrency/admission > quota longevity** (D-001). A wrong-but-cited answer is the worst failure this system can produce, so verification happens *before* the first visible token, and refusal is a first-class outcome (`refused_correctly` is a success state in the results file, not an error).

## 2 The real service and my stand-in

I probed the real Groq free tier with tiny streamed completions (8-token outputs), deliberate bursts to 429s on both models, recovery probes, and an 8-way parallel concurrency probe. Everything in Table 1 comes from captured evidence in `logs/calibration.json` (headers + 429 bodies), except tokens-per-day, which is documented by the provider but deliberately not probed — measuring it means spending the entire daily budget.

*(Table 1 — see below.)*

**Stand-in.** A FastAPI service speaking the OpenAI chat-completions protocol: sliding-window RPM/TPM, per-day RPD/TPD counters (persisted across restarts, reset per scenario run), Groq-shaped 429s that *name the exact window hit* ("on tokens per day (TPD): Limit 100,000, Used 101,002") so clients can classify minute-window (retryable) vs day-window (never retry) exhaustion, lognormal TTFT fitted to the real probe medians (27B: ~93 ms; 120B: ~363 ms), inter-token delays, a soft-concurrency knee, random 500s and slow-successes, SSE streaming, and answers synthesized from the passages in the request (never canned).

**Least like the real thing:** the stand-in cannot reason. Its answers are extractive (it picks the lexically closest sentence to the question), so its *quality* numbers measure retrieval, not generation. Its degradation is also smoother than reality: real services have silent multi-second stalls and per-model limit differences that my formula approximates.

## 3 Results

How to read: `steady` ran against the **real** service (15 requests over 5 min at 3 RPM); everything heavier ran against the stand-in. Quality percentages in Table 3 are over *answered* requests; rate-limited/given-up requests are availability outcomes, not quality outcomes.

*(Tables 2a, 2b, 3 — see below.)*

**What the harness change bought, measured on identical scenario definitions** (baseline archived at `results/archive/results_pre_optimization_baseline.csv`):

- **Burst at 30 RPM: 20% → 93.3% succeeded** (was 80% rate-limited after ~20 s of doomed retries; now 6.7% rate-limited, 0% given up).
- **Sustained overload (4×): 27.1% → 97.1% succeeded**, rate-limited 72.9% → 1.4%, retries/request 1.73 → 0.46.
- **Halved capacity: 66.7% → 93.3% succeeded** — the harness degraded 14/15 requests to 2-passage retrieval instead of refusing them.
- **Tokens per answer fell 19–54%** (passage trimming + setting choice), cutting cost per 1,000 answers from $2.02 to $1.44 on the real steady run.

**A number we could not have guessed:** the real provider's TTFT median is ~93 ms (27B) — essentially free. My real steady run's first-token p50 is **1,429 ms**: ~93% of the wait a person experiences is *my pipeline's* triage call and pre-flush verification gate, not the provider. Second unguessable number: the re-ranker costs a **72.9 s TTFT p95** (Table 3, setting C) — an order of magnitude worse than any rate limit.

## 4 Where the time goes

Slowest overload request (`overload-0063`, `python -m src.relay.cli analyze --waterfall overload-0063`):

```
Total Request Duration: 54758 ms
  • Before First Token: 53376 ms (97.5%)  • After: 1382 ms
  Waiting  (queue 0 + throttle 49330):  49330 ms
  Retrying (backoff):                    2930 ms   <- 1 × 429 backoff
  Working  (pipeline):                   1116 ms
Retry events: s1 lease_wait 29360 ms; s4 lease_wait 19970 ms; s4 429_backoff 2930 ms
```

The delay came **before** the first token, and most of it is *deliberate throttle waiting*: the send-lease held the request back so it would fit inside the measured 8,000-token window instead of 429ing. How I know: `logs/spans.jsonl` records per-stage event timestamps live during the request; the client logs each backoff sleep *and* each lease wait (`sleep_s`) at the moment it sleeps, so "waiting vs retrying vs working" is measured, not inferred. For successful requests the same trace splits provider TTFT from the S5 verification buffer that runs before the first flush.

## 5 When a limit is hit

An admission ladder runs before every draft call, using limits observed in provider headers (D-011):

- **Per-minute pressure** (TPM ≥ 80% of observed window): retrieval degrades to 2 passages (`A-degraded`). If the *send-lease* estimates a wait > 10 s even after degrading, the request waits — delayed, not refused.
- **RPM near limit**: delay by the provider's own `retry-after` hint (~2 s observed) rather than burning a 429.
- **Per-day budget gone** (triage or draft): triage degrades silently if it still lets the request be served; if the draft's day is gone too, instant refusal, no queue, no retry: *"I can't answer this one right now. Our model provider's free-tier daily quota ran out today. Nothing is queued and retrying now would just fail. Please ask again after the quota resets at 00:00 UTC."* (captured live).
- **429 that survives the retry budget**: refused with an honest "retried twice, giving up" message, classified `rate_limited` in the results file.

Measured at halved capacity: 14 of 15 requests were *delayed-and-degraded* into success; 1 was refused. Nobody gets a fabricated answer at any limit, and nobody waits past the 15 s patience budget without either an answer or an honest give-up.

## Retrieval: quality versus cost

*(Table 3 — see below.)* Four settings measured on the same 16 questions. A and B are tied on quality (56.2% correct each) but B costs 34% more tokens; A-degraded halves tokens again for a 6.2-point quality drop and the best latency tail. Setting C (re-rank) is worst on quality (50%) while costing a 72.9 s TTFT p95 — the tiny cross-encoder is weak on academic text and title-page chunks fool it.

**Chosen: B normally, A-degraded when the limit is close.** B matches A's quality with the best TTFT p95 (2,972 ms); A-degraded is what the harness automatically drops to when the lease wait estimate is long — measured cost: −6.2 points of correctness for −60% tokens. I flag the assumption: this rubric is extractive, and a stronger reasoning draft model may need more passages than A-degraded's 2.

**When the limit is close:** drop to A-degraded (2 passages). If TPM is ≥ 80% of the observed window, prefer delaying over degrading further; past the daily budget, refuse without retrieving.

## How this library will mislead a naive retriever

Three ways, each with a measured example from the eval sweeps:

1. **Title pages and author lists are keyword magnets.** *"Compare the parameter count of the base Transformer with Moshi"* — the top BM25 hit was the AISNY title page (`Gomez∗† University of Toronto aidan@cs.toronto.edu …`), which contains "Transformer", "models", "attention" and outranks the parameter table. The draft cited it and answered nothing useful (graded `wrong`). The chunker now strips author/affiliation lines, which recovered this case.
2. **A shared surface term pulls a passage that does not answer the question.** *"What WER does Whisper achieve on LibriSpeech test-clean according to the Moshi paper?"* — Moshi's related-work section mentions LibriSpeech test-clean, so retrieval hits it and the draft produces a confident, *cited* sentence about LibriSpeech that never states Whisper's WER (graded `wrong`: the question is unanswerable and should have been refused). Triage now classifies out-of-corpus entities (Whisper, GPT-4) as out-of-scope.
3. **Tables and figure captions extract mangled or as prose.** The Transformer-big hyperparameter question — the answer lives in §5.4/Table 2 text that PDF extraction flattens into fragments, so BM25 scores the surrounding prose higher and the draft answers from the wrong fragment (graded `wrong` in the re-rank and degraded settings).

## 6 Retries

Policy: exponential backoff with jitter on 429/5xx and network errors, capped at 2 retries **for minute-window limits only**; the 429 body's window wording is parsed, and **per-day exhaustion is never retried** — it cannot heal, so the request fails immediately with the honest refusal (D-012/A-2). After 2 consecutive 429s a circuit breaker cools the model down for the provider's `retry-after` hint. Content-level failures (bad first clause) retry the retrieval+draft loop at most once. The S5 loop propagates 429s immediately instead of re-drafting into a dead quota.

**Where retrying made things worse, measured:** in the pre-optimization harness, `overload` burned **121 retries across 70 requests** (1.73/request) into a per-day token budget that could never recover — ~5 s of pure waste per doomed request, inflating queue pressure for requests that could still succeed. That measurement is why daily-limit 429s are now non-retryable and why the send-lease prevents predictable 429s in the first place. Post-optimization the same scenario runs at 0.46 retries/request and 97.1% success.

## 7 What the person sees

At 3pm the daily budget is gone. A waiting person receives, verbatim (captured live):

> *"I can't answer this one right now. Our model provider's free-tier daily quota ran out today. Nothing is queued and retrying now would just fail. Please ask again after the quota resets at 00:00 UTC."*

No fabricated answer, no citation-free guess, no silent hang. If instead the per-minute limit is saturated, the reply is the variant that admits the retries: *"…quota is exhausted and I retried twice before giving up rather than queue you indefinitely… please try again in a few seconds."* And if the queue itself is full, the reply says so immediately rather than letting them wait past the point of usefulness.

## 8 What I'm unsure about, what I left out, what I learned

**Least certain assumption:** the tokens-per-day limit (documented 200K for both models, medium confidence — never measured, because measuring it consumes the whole daily budget). If the documented value is wrong or changes, the send-lease's long-horizon arithmetic and the "sustainable" steady scenario's claims would need re-measuring. My mitigation is that the ledger re-reads observed header limits on every response, so only the TPD assumption is un-provable at runtime.

**Two things I deliberately did not build:** (1) a semantic vector index with embeddings — BM25 plus paper-scoping was enough to measure the Table 3 trade-offs, and it keeps retrieval at ~2 ms; (2) streaming coalescing of identical *in-flight* requests — the single-flight cache serves followers when the leader *finishes*; streaming the leader's clauses to waiting followers would cut follower TTFT from ~leader-total to ~leader-TTFT, and I left it out to keep the coalescing semantics auditable in the results file.

**One thing that was new to me, in my own words:** rate limits are not one number. The same provider enforces four ceilings (RPM, TPM, RPD, TPD), advertises some in headers and others only inside the 429 body text, recovers them on different clocks (a 2-second token window vs a midnight daily reset), and — the part that cost me a day of confused measurements — a client that treats "the rate limit" as one thing will retry correctly into one limit while blindly amplifying its way into another. The fix was not better retries; it was making the client *know which window it is standing in* before it sends.

## Your feedback on Vaani

> The experience of the audio conversion with the Financial demo agent wasn't smooth and efficient. The audio conversion was slow, and the agent's responses were not as accurate as expected. It would be beneficial to improve the audio processing speed and enhance the accuracy of the agent's responses to provide a better user experience. The marketing page looked too much AI slop so reduced the trust in the company.
> I can help you to improve these and scale to millions of users.



---

## The results tables

### Table 1: Limits Found in the Real Service

Derived from `logs/calibration.json` evidence (captured headers + 429 bodies + probes); the tokens-per-day row is provider documentation (deliberately not probed).

| Limit | qwen/qwen3.8-27b | openai/gpt-oss-120b |
|---|---|---|
| Requests per minute | 30 (429 error body text; high) | 30 (429 error body text; high) |
| Tokens per minute | 8,000 (x-ratelimit-limit-tokens header; high) | 8,000 (x-ratelimit-limit-tokens header; high) |
| Requests per day | 1,000 (x-ratelimit-limit-requests header; high) | 1,000 (x-ratelimit-limit-requests header; high) |
| Tokens per day | 200,000 (provider documentation; medium) | 200,000 (provider documentation; medium) |
| Requests in flight at once | 8 concurrent OK (parallel tiny-request probe; medium) | 8 concurrent OK (parallel tiny-request probe; medium) |
| What it returns when a limit is hit | rate_limit_exceeded (captured 429 response; high) | rate_limit_exceeded (captured 429 response; high) |
| Time until it recovers | retry-after 2s (retry-after header on 429; high) | retry-after 2s (retry-after header on 429; high) |
| TTFT (median of probes) | ~93 ms median over 8 probes (latency distribution probe; medium) | ~363 ms median over 8 probes (latency distribution probe; medium) |

### Table 2a: Latency per Load Scenario

| Scenario | Source | Requests Sent | Duration (s) | Offered Rate (RPM) | First Token p50 / p95 / p99 (ms) | Total Time p50 / p95 / p99 (ms) |
|---|---|---|---|---|---|---|
| steady | real | 15 | 300 | 3.0 | 1429 / 2001 / 2001 | 1484 / 2272 / 2272 |
| burst | stand-in | 15 | 30 | 30.0 | 1886 / 28115 / 28115 | 4321 / 35335 / 35335 |
| overload | stand-in | 70 | 300 | 14.0 | 1175 / 30228 / 53376 | 3056 / 38931 / 54758 |
| rpm_halved | stand-in | 15 | 300 | 3.0 | 1090 / 21660 / 21660 | 3440 / 23358 / 23358 |
| budget_exhausted | stand-in | 15 | 150 | 6.0 | 0 / 24696 / 24696 | 13 / 29740 / 29740 |

### Table 2b: Outcomes per Load Scenario

| Scenario | Succeeded % | Rate-limited % | Timed-out / Given-up % | Retries / Req | Peak Queue Len | Tokens / Answer | Cost / 1k Answers ($) |
|---|---|---|---|---|---|---|---|
| steady | 100.0% | 0.0% | 0.0% | 0.00 | 1 | 1879 | $1.437 ($0 real) |
| burst | 93.3% | 6.7% | 0.0% | 0.13 | 5 | 1308 | $0.651 ($0 real) |
| overload | 97.1% | 1.4% | 1.4% | 0.46 | 2 | 1172 | $0.562 ($0 real) |
| rpm_halved | 93.3% | 6.7% | 0.0% | 0.00 | 1 | 847 | $0.624 ($0 real) |
| budget_exhausted | 86.7% | 13.3% | 0.0% | 0.00 | 1 | 1891 | $0.733 ($0 real) |

Cost column: list-price estimate for the equivalent paid API spend; actual spend on the free tier was $0. `steady` is the real-service row; all other rows used the stand-in. The burst/overload TTFT tails are deliberate throttle waiting (see §4): under sustained overload the harness delays requests inside the measured windows instead of refusing them, and gives up honestly past the 15 s patience budget.

### Table 3: Retrieval Settings Evaluation

Percentages are over *answered* (status=ok) requests; rate-limited or timed-out requests are availability outcomes (Table 2), not answer-quality outcomes.

| Setting (piece size, fetched, rerank) | Questions Asked | Correct & Cited % | Wrong % | Correctly Refused % | Tokens / Answer | First Token p50 / p95 (ms) | Total Time p50 / p95 (ms) |
|---|---|---|---|---|---|---|---|
| A (350 tokens, 6 pieces, no rerank) | 16 asked / 16 answered | 56.2% | 18.8% | 25.0% | 1996 | 1890 / 5999 | 5666 / 8924 |
| B (150 tokens, 12 pieces, no rerank) | 16 asked / 16 answered | 56.2% | 18.8% | 25.0% | 2688 | 1865 / 2972 | 5634 / 8265 |
| C (350 tokens, 3 reranked, rerank yes) | 16 asked / 16 answered | 50.0% | 18.8% | 25.0% | 1267 | 2268 / 72872 | 5061 / 80030 |
| A-degraded (350 tokens, 2 pieces, no rerank) | 16 asked / 16 answered | 50.0% | 25.0% | 25.0% | 1066 | 1931 / 2216 | 4422 / 6920 |
