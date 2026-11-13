import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional

import httpx

try:
    from relay.analyze import compute_table_1
    from relay.config import (
        GROQ_BASE_URL,
        LOGS_DIR,
        MODEL_DRAFT,
        MODEL_TRIAGE,
        STANDIN_BASE_URL,
    )
except ImportError:
    from src.relay.analyze import compute_table_1
    from src.relay.config import (
        GROQ_BASE_URL,
        LOGS_DIR,
        MODEL_DRAFT,
        MODEL_TRIAGE,
        STANDIN_BASE_URL,
    )

CALIBRATION_FILE = LOGS_DIR / "calibration.json"


async def run_calibration_suite(max_requests: int = 45, force_standin: bool = False, latency_samples: int = 10) -> None:
    api_key = os.getenv("GROQ_API_KEY", "")
    use_standin = force_standin or not api_key

    base_url = STANDIN_BASE_URL if use_standin else GROQ_BASE_URL
    headers = {"Content-Type": "application/json"}
    if api_key and not use_standin:
        headers["Authorization"] = f"Bearer {api_key}"

    print(
        f"=== Starting Phase 1 Calibration against {'Stand-in' if use_standin else 'Real Groq API'} ({base_url}) ==="
    )

    records = []
    if CALIBRATION_FILE.exists():
        try:
            with open(CALIBRATION_FILE, "r", encoding="utf-8") as f:
                records = json.load(f)
            if not isinstance(records, list):
                records = [records]
        except Exception:
            records = []

    async with httpx.AsyncClient(timeout=15.0) as client:
        # Step 1: Baseline TTFT and limit header telemetry (spaced requests)
        print("\n[Step 1/4] Gathering baseline latency and rate-limit headers (spaced)...")
        models_to_test = [MODEL_TRIAGE, MODEL_DRAFT]

        for model in models_to_test:
            print(f"  Testing model: {model}...", end=" ", flush=True)
            payload = {
                "model": model,
                "messages": [{"role": "user", "content": "Reply with one word: ping."}],
                "max_tokens": 10,
                "temperature": 0.0,
            }

            t0 = time.time()
            try:
                resp = await client.post(f"{base_url}/chat/completions", headers=headers, json=payload)
                elapsed_ms = int((time.time() - t0) * 1000)

                rate_headers = {
                    k: v for k, v in resp.headers.items() if k.lower().startswith("x-ratelimit-") or k.lower() == "retry-after"
                }

                print(f"Status {resp.status_code} in {elapsed_ms}ms | Headers captured: {len(rate_headers)}")

                rec = {
                    "timestamp": time.time(),
                    "phase": "baseline_telemetry",
                    "model": model,
                    "status_code": resp.status_code,
                    "latency_ms": elapsed_ms,
                    "headers": rate_headers,
                    "body": resp.text[:500],
                }
                records.append(rec)

            except Exception as e:
                print(f"Error: {e}")

            await asyncio.sleep(1.5)

            # Per-model latency distribution probe (streamed, tiny)
            print(f"  Latency distribution on {model} ({latency_samples} streamed probes)...", end=" ", flush=True)
            for i in range(latency_samples):
                payload = {
                    "model": model,
                    "messages": [{"role": "user", "content": "Reply with exactly one word: ok"}],
                    "max_tokens": 8,
                    "temperature": 0.0,
                    "stream": True,
                }
                t0 = time.time()
                ttft_ms = None
                try:
                    async with client.stream(
                        "POST", f"{base_url}/chat/completions", headers=headers, json=payload
                    ) as resp:
                        if resp.status_code == 200:
                            async for line in resp.aiter_lines():
                                line = line.strip()
                                if line.startswith("data: ") and ttft_ms is None:
                                    ttft_ms = int((time.time() - t0) * 1000)
                            total_ms = int((time.time() - t0) * 1000)
                            records.append(
                                {
                                    "timestamp": t0,
                                    "phase": "latency_distribution",
                                    "model": model,
                                    "status_code": 200,
                                    "ttft_ms": ttft_ms,
                                    "total_ms": total_ms,
                                }
                            )
                            sys.stdout.write(".")
                            sys.stdout.flush()
                        else:
                            print(f" status {resp.status_code}", end=" ")
                except Exception as e:
                    print(f" probe error: {e}", end=" ")
                await asyncio.sleep(1.2)
            print()

            # Per-model concurrency probe: fire 8 parallel tiny requests
            print(f"  Concurrency probe on {model} (8 parallel tiny requests)...", end=" ", flush=True)
            conc_payload = {
                "model": model,
                "messages": [{"role": "user", "content": "ok"}],
                "max_tokens": 5,
                "temperature": 0.0,
            }

            async def _one_call():
                t0 = time.time()
                try:
                    r = await client.post(f"{base_url}/chat/completions", headers=headers, json=conc_payload)
                    return r.status_code, int((time.time() - t0) * 1000)
                except Exception:
                    return -1, int((time.time() - t0) * 1000)

            results_c = list(await asyncio.gather(*[_one_call() for _ in range(8)]))
            ok_c = sum(1 for s, _ in results_c if s == 200)
            codes = [s for s, _ in results_c]
            records.append(
                {
                    "timestamp": time.time(),
                    "phase": "concurrency_probe",
                    "model": model,
                    "status_codes": codes,
                    "succeeded": ok_c,
                    "in_flight_observed": ok_c,
                    "note": "8 simultaneous tiny requests; how many completed OK",
                }
            )
            print(f" {ok_c}/8 OK (codes: {codes})")
            await asyncio.sleep(2.0)

        # Step 2: Deliberate burst probes on each model to capture 429 ladders
        burst_cap = min(max_requests, 35)
        probe_payload = {
            "messages": [{"role": "user", "content": "Hi"}],
            "max_tokens": 5,
            "temperature": 0.0,
        }

        for burst_model in models_to_test:
            probe_payload["model"] = burst_model
            print(f"\n[Step 2/4] Firing burst probe on {burst_model} to observe 429...")

            encountered_429 = False
            hit_index = 0

            for i in range(burst_cap):
                t0 = time.time()
                try:
                    resp = await client.post(f"{base_url}/chat/completions", headers=headers, json=probe_payload)
                    elapsed_ms = int((time.time() - t0) * 1000)

                    if resp.status_code == 429:
                        print(f"\n  SUCCESS: 429 captured on probe #{i+1} in {elapsed_ms}ms!")
                        encountered_429 = True
                        hit_index = i + 1

                        rate_headers = {k: v for k, v in resp.headers.items()}
                        rec = {
                            "timestamp": time.time(),
                            "phase": "deliberate_429_probe",
                            "model": burst_model,
                            "status_code": 429,
                            "latency_ms": elapsed_ms,
                            "headers": rate_headers,
                            "body": resp.text,
                        }
                        records.append(rec)
                        break
                    else:
                        sys.stdout.write(".")
                        sys.stdout.flush()

                except Exception as e:
                    print(f"\n  Probe error: {e}")
                    break

                await asyncio.sleep(0.05)

            # Step 3: Recovery probe
            if encountered_429:
                print(f"\n[Step 3/4] Probing recovery after 429 until 200 returns...")
                for attempt in range(10):
                    await asyncio.sleep(2.0)
                    t0 = time.time()
                    try:
                        resp = await client.post(f"{base_url}/chat/completions", headers=headers, json=probe_payload)
                        elapsed_ms = int((time.time() - t0) * 1000)
                        records.append(
                            {
                                "timestamp": t0,
                                "phase": "recovery_probe",
                                "model": burst_model,
                                "status_code": resp.status_code,
                                "latency_ms": elapsed_ms,
                                "headers": {k: v for k, v in resp.headers.items() if k.lower().startswith("x-ratelimit-") or k.lower() == "retry-after"},
                            }
                        )
                        if resp.status_code == 200:
                            print(f"  Service recovered to HTTP 200 after ~{(attempt+1)*2}s!")
                            break
                        else:
                            print(f"  Attempt {attempt+1}: Status {resp.status_code}...")
                    except Exception:
                        pass
            else:
                print(f"\n  (Burst on {burst_model} completed without hitting 429 ceiling within {burst_cap} requests)")

    CALIBRATION_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(CALIBRATION_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, indent=2)

    print(f"\nCalibration artifact saved to {CALIBRATION_FILE} ({len(records)} total records).")
    print("\n" + compute_table_1())
