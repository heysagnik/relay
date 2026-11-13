import argparse
import asyncio
import contextlib
import csv
import json
import os
import signal
import sys
import time
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator, Dict, List, Optional

import httpx
import uvicorn

try:
    from relay.analyze import compute_table_1, compute_table_3, compute_tables_2a_2b, load_results_rows, print_waterfall
    from relay.budget import BudgetLedger
    from relay.calibrate import run_calibration_suite
    from relay.config import (
        BASE_DIR,
        DATA_DIR,
        GROQ_BASE_URL,
        LOGS_DIR,
        RESULTS_DIR,
        STANDIN_BASE_URL,
        STANDIN_CONFIG_URL,
        STANDIN_HOST,
        STANDIN_PORT,
        STANDIN_STATS_URL,
    )
    from relay.grade import AnswerEvaluator, run_evaluation_sweep
    from relay.groq_client import GroqClient
    from relay.loadgen import OpenLoopLoadGenerator
    from relay.orchestrator import RelayOrchestrator, RequestContext
    from relay.retrieval import RetrievalEngine
except ImportError:
    from src.relay.analyze import compute_table_1, compute_table_3, compute_tables_2a_2b, load_results_rows, print_waterfall
    from src.relay.budget import BudgetLedger
    from src.relay.calibrate import run_calibration_suite
    from src.relay.config import (
        BASE_DIR,
        DATA_DIR,
        GROQ_BASE_URL,
        LOGS_DIR,
        RESULTS_DIR,
        STANDIN_BASE_URL,
        STANDIN_CONFIG_URL,
        STANDIN_HOST,
        STANDIN_PORT,
        STANDIN_STATS_URL,
    )
    from src.relay.grade import AnswerEvaluator, run_evaluation_sweep
    from src.relay.groq_client import GroqClient
    from src.relay.loadgen import OpenLoopLoadGenerator
    from src.relay.orchestrator import RelayOrchestrator, RequestContext
    from src.relay.retrieval import RetrievalEngine

RESULTS_CSV = RESULTS_DIR / "results.csv"
BUDGET_JSON = DATA_DIR / "budget.json"


@asynccontextmanager
async def managed_standin_server(
    host: str = STANDIN_HOST, port: int = STANDIN_PORT, needed: bool = True
) -> AsyncGenerator[None, None]:
    if not needed:
        yield
        return

    stats_url = f"http://{host}:{port}/__stats"
    already_up = False

    try:
        async with httpx.AsyncClient(timeout=0.6) as client:
            resp = await client.get(stats_url)
            if resp.status_code == 200:
                already_up = True
    except Exception:
        already_up = False

    if already_up:
        yield
        return

    config = uvicorn.Config(
        "relay.standin_server:app",
        host=host,
        port=port,
        log_level="warning",
    )
    server = uvicorn.Server(config)
    server_task = asyncio.create_task(server.serve())

    start_wait = time.time()
    ready = False

    async with httpx.AsyncClient(timeout=0.3) as client:
        while time.time() - start_wait < 5.0:
            try:
                resp = await client.get(stats_url)
                if resp.status_code == 200:
                    ready = True
                    break
            except Exception:
                pass
            await asyncio.sleep(0.05)

    if not ready:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=1.0)
        except Exception:
            server_task.cancel()
        raise RuntimeError(f"Stand-in server failed to start at http://{host}:{port} within 5.0s")

    try:
        yield
    finally:
        server.should_exit = True
        try:
            await asyncio.wait_for(server_task, timeout=3.0)
        except Exception:
            server_task.cancel()


def print_telemetry_footer(ctx: RequestContext) -> None:
    events = dict(ctx.events)
    t_enq = events.get("enqueue", 0)
    t_deq = events.get("dequeue", t_enq)
    t_s1_start = events.get("s1_start", t_deq)
    t_s1_end = events.get("s1_end", t_s1_start)
    t_s2_start = events.get("s2_start", t_s1_end)
    t_s2_end = events.get("s2_end", t_s2_start)
    t_s4_start = events.get("s4_start_attempt_1", events.get("s4_start", t_s2_end))
    t_s4_first_tok = events.get("s4_provider_first_token", t_s4_start)
    t_s5_flush = events.get("s5_first_flush", t_s4_first_tok)

    q_wait = max(0, t_deq - t_enq)
    s1_dur = max(0, t_s1_end - t_s1_start)
    s2_dur = max(0, t_s2_end - t_s2_start)
    s4_tok = max(0, t_s4_first_tok - t_s4_start)
    s5_tax = max(0, t_s5_flush - t_s4_first_tok)

    waterfall_str = (
        f"queue={q_wait}ms -> s1={s1_dur}ms -> s2={s2_dur}ms -> "
        f"s4_first_tok={s4_tok}ms -> s5_first_flush={s5_tax}ms -> total={ctx.total_ms}ms"
    )

    refusal_info = f", Refusal: {ctx.refusal_reason}" if ctx.refusal_reason else ""
    grade_info = f", Grade: {ctx.answer_grade}" if ctx.answer_grade else ""

    sys.stderr.write("\n" + "─" * 70 + "\n")
    sys.stderr.write(
        f"[TELEMETRY] Request: {ctx.request_id} | Source: {ctx.source} | Setting: {ctx.retrieval_setting} | Status: {ctx.status}{grade_info} | Retries: {len(ctx.retries)}\n"
    )
    sys.stderr.write(
        f"  Latency: TTFT (first flush): {ctx.first_token_ms if ctx.first_token_ms is not None else '-'} ms | Total: {ctx.total_ms} ms | Tokens: in={ctx.tokens_in}, out={ctx.tokens_out}\n"
    )
    sys.stderr.write(
        f"  S5 Verification: Clauses: {ctx.clauses_verified} | Loop: {ctx.loop_triggered}{refusal_info}\n"
    )
    sys.stderr.write(f"  Waterfall: {waterfall_str}\n")
    sys.stderr.write("─" * 70 + "\n")
    sys.stderr.flush()


def append_csv_result(ctx: RequestContext) -> None:
    row = [
        ctx.request_id,
        ctx.scenario,
        ctx.source,
        datetime.fromtimestamp(ctx.enqueue_time, timezone.utc).isoformat(),
        str(ctx.first_token_ms) if ctx.first_token_ms is not None else "",
        str(ctx.total_ms),
        ctx.status,
        str(len(ctx.retries)),
        str(ctx.tokens_in),
        str(ctx.tokens_out),
        ctx.retrieval_setting,
        ctx.answer_grade or "",
    ]
    with open(RESULTS_CSV, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(row)


def create_orchestrator(source: str) -> RelayOrchestrator:
    base_url = STANDIN_BASE_URL if source == "stand-in" else GROQ_BASE_URL
    # Separate ledgers: real and stand-in are different providers with
    # different observed limits; sharing one file cross-contaminates admission.
    ledger = BudgetLedger(path=DATA_DIR / ("budget.json" if source == "real" else "budget_standin.json"))
    retrieval = RetrievalEngine()
    client = GroqClient(base_url=base_url, budget_ledger=ledger, source_name=source)
    return RelayOrchestrator(
        llm_client=client,
        retrieval_engine=retrieval,
        budget_ledger=ledger,
    )


async def execute_ask(
    question: str,
    source: str = "stand-in",
    setting: str = "A",
    request_id: Optional[str] = None,
    orchestrator: Optional[RelayOrchestrator] = None,
) -> RequestContext:
    orch = orchestrator or create_orchestrator(source)
    req_id = request_id or f"ask-{int(time.time()*1000)%100000:05d}"

    def on_flush(token_or_clause: str):
        sys.stdout.write(token_or_clause)
        sys.stdout.flush()

    ctx = await orch.handle_request(
        request_id=req_id,
        question=question,
        scenario="ask",  # interactive asks are not load-scenario runs; keeps Table 2 clean
        retrieval_setting=setting,
        on_flush=on_flush,
        priority=1,  # interactive users outrank generated load (Phase B4)
    )
    if orchestrator is None:
        await orch.client.aclose()

    sys.stdout.write("\n")
    sys.stdout.flush()

    print_telemetry_footer(ctx)
    append_csv_result(ctx)
    return ctx


async def run_shell_repl(initial_source: str = "stand-in", initial_setting: str = "A") -> None:
    current_source = initial_source
    current_setting = initial_setting
    orchestrator = create_orchestrator(current_source)

    print("=" * 70)
    print("The Relay — Interactive Shell")
    print(f"Stand-in Server listening on http://{STANDIN_HOST}:{STANDIN_PORT}")
    print(f"Active Source: {current_source} | Active Setting: {current_setting}")
    print("Commands: :source <real|stand-in>, :setting <A|B|C|A-degraded>, :stats, :quit")
    print("=" * 70)

    loop = asyncio.get_event_loop()

    while True:
        try:
            sys.stdout.write(f"\nrelay [{current_source}|{current_setting}]> ")
            sys.stdout.flush()
            line = await loop.run_in_executor(None, sys.stdin.readline)
            if not line:
                break

            cmd = line.strip()
            if not cmd:
                continue

            if cmd in (":quit", ":exit", "quit", "exit"):
                await orchestrator.client.aclose()
                print("Exiting Relay Shell.")
                break

            if cmd.startswith(":source"):
                parts = cmd.split(maxsplit=1)
                if len(parts) > 1 and parts[1].strip() in ("real", "stand-in"):
                    await orchestrator.client.aclose()
                    current_source = parts[1].strip()
                    orchestrator = create_orchestrator(current_source)
                    print(f"Switched source to: {current_source}")
                else:
                    print("Usage: :source <real|stand-in>")
                continue

            if cmd.startswith(":setting"):
                parts = cmd.split(maxsplit=1)
                if len(parts) > 1 and parts[1].strip() in ("A", "B", "C", "A-degraded"):
                    current_setting = parts[1].strip()
                    print(f"Switched setting to: {current_setting}")
                else:
                    print("Usage: :setting <A|B|C|A-degraded>")
                continue

            if cmd == ":stats":
                print("\n[Stand-in Server Metrics]")
                try:
                    async with httpx.AsyncClient(timeout=2.0) as client:
                        resp = await client.get(STANDIN_STATS_URL)
                        if resp.status_code == 200:
                            print(json.dumps(resp.json(), indent=2))
                        else:
                            print(f"Failed to fetch stats: HTTP {resp.status_code}")
                except Exception as e:
                    print(f"Error connecting to stand-in server: {e}")

                print("\n[Budget Ledger State]")
                if BUDGET_JSON.exists():
                    try:
                        with open(BUDGET_JSON, "r", encoding="utf-8") as f:
                            print(f.read())
                    except Exception as e:
                        print(f"Error reading budget.json: {e}")
                continue

            # Standard question flow
            await execute_ask(
                question=cmd,
                source=current_source,
                setting=current_setting,
                orchestrator=orchestrator,
            )

        except (KeyboardInterrupt, EOFError):
            print("\nExiting Relay Shell.")
            break
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)


async def main_async(args: argparse.Namespace) -> None:
    sub = args.subcommand

    if sub is None or sub == "shell" or getattr(args, "interactive", False):
        source = getattr(args, "source", "stand-in")
        setting = getattr(args, "setting", "A")
        async with managed_standin_server(needed=True):
            await run_shell_repl(initial_source=source, initial_setting=setting)
        return

    if sub == "ask":
        needed = (args.source == "stand-in")
        async with managed_standin_server(needed=needed):
            await execute_ask(
                question=args.question,
                source=args.source,
                setting=args.setting,
                request_id=args.request_id,
            )
        return

    if sub == "calibrate":
        api_key = os.getenv("GROQ_API_KEY", "")
        needed = not bool(api_key)
        async with managed_standin_server(needed=needed):
            await run_calibration_suite(
                max_requests=args.max_requests,
                force_standin=needed,
                latency_samples=args.latency_samples,
            )
        return

    if sub == "eval":
        needed = (args.source == "stand-in")
        async with managed_standin_server(needed=needed):
            if needed:
                # Fresh provider state per invocation keeps the sweep inside
                # the stand-in's calibrated daily budget.
                try:
                    async with httpx.AsyncClient(timeout=2.0) as client:
                        cfg_resp = await client.get(STANDIN_STATS_URL)
                        models = cfg_resp.json().get("models", {})
                        await client.post(
                            STANDIN_BASE_URL.replace("/v1", "/__state"),
                            json={"models": {m: {"requests_day": 0, "tokens_day": 0} for m in models}},
                        )
                except Exception as e:
                    print(f"Warning: could not reset stand-in state: {e}")
            orch = create_orchestrator(args.source)
            try:
                await run_evaluation_sweep(
                    orchestrator=orch,
                    settings=args.settings,
                    questions_file=Path(args.questions),
                )
            finally:
                await orch.client.aclose()
            return

    if sub == "load":
        target_source = args.source
        if target_source == "from-config":
            target_source = "real" if args.scenario == "steady" else "stand-in"

        needed = (target_source == "stand-in")

        async with managed_standin_server(needed=needed):
            if needed:
                # Deterministic scenario starts: reset the stand-in's daily
                # counters (persistence exists for restart continuity, not for
                # carrying one scenario's exhaustion into the next run).
                try:
                    async with httpx.AsyncClient(timeout=2.0) as client:
                        cfg_resp = await client.get(STANDIN_STATS_URL)
                        models = cfg_resp.json().get("models", {})
                        await client.post(
                            STANDIN_BASE_URL.replace("/v1", "/__state"),
                            json={"models": {m: {"requests_day": 0, "tokens_day": 0} for m in models}},
                        )
                except Exception as e:
                    print(f"Warning: could not reset stand-in state: {e}")

            if needed and args.scenario == "rpm_halved":
                try:
                    async with httpx.AsyncClient(timeout=2.0) as client:
                        cfg_resp = await client.get(STANDIN_CONFIG_URL)
                        if cfg_resp.status_code == 200:
                            cfg = cfg_resp.json()
                            for m in cfg.get("models", {}):
                                cfg["models"][m]["rpm_limit"] = max(1, int(cfg["models"][m]["rpm_limit"] / 2))
                                cfg["models"][m]["tpm_limit"] = max(500, int(cfg["models"][m]["tpm_limit"] / 2))
                            await client.post(STANDIN_CONFIG_URL, json=cfg)
                            print("[Stand-in] Reconfigured for 'rpm_halved': limits divided by 2.")
                except Exception as e:
                    print(f"Warning: could not configure stand-in limits: {e}")

            if needed and args.scenario == "budget_exhausted":
                # The scenario is '3pm: the daily budget is mostly gone' — seed the
                # stand-in to 90% of its (half-quota) daily limits, then watch the
                # transition into refusal across the run.
                try:
                    async with httpx.AsyncClient(timeout=2.0) as client:
                        cfg_resp = await client.get(STANDIN_CONFIG_URL)
                        limits = cfg_resp.json().get("models", {})
                        seed = {
                            "models": {
                                m: {
                                    "requests_day": int(limits[m]["rpd_limit"] * 0.9),
                                    "tokens_day": int(limits[m]["tpd_limit"] * 0.9),
                                }
                                for m in limits
                            }
                        }
                        await client.post(STANDIN_BASE_URL.replace("/v1", "/__state"), json=seed)
                        print("[Stand-in] Seeded daily counters to 90% for 'budget_exhausted'.")
                except Exception as e:
                    print(f"Warning: could not seed stand-in state: {e}")

            orch = create_orchestrator(target_source)
            try:
                generator = OpenLoopLoadGenerator(orchestrator=orch)
                if args.scenario == "all":
                    for sc in ["steady", "burst", "overload", "rpm_halved", "budget_exhausted"]:
                        await generator.run_scenario(sc)
                else:
                    await generator.run_scenario(args.scenario)
            finally:
                await orch.client.aclose()
        return

    if sub == "analyze":
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
            print(f"Verification SUCCESS: results.csv schema valid ({len(rows)} data rows). Recomputation ready.")
        return


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="relay",
        description="The Relay — Research Paper RAG Assistant CLI",
    )
    parser.add_argument(
        "-i", "--interactive", action="store_true", help="Launch interactive shell REPL"
    )

    subparsers = parser.add_subparsers(dest="subcommand", help="Available subcommands")

    # 1. ask
    p_ask = subparsers.add_parser("ask", help="Query the assistant with a single question")
    p_ask.add_argument("question", type=str, help="Question to ask the research library")
    p_ask.add_argument(
        "--source", choices=["real", "stand-in"], default="stand-in", help="Model service backend"
    )
    p_ask.add_argument(
        "--setting",
        choices=["A", "B", "C", "A-degraded"],
        default="A",
        help="Retrieval setting",
    )
    p_ask.add_argument("--request-id", type=str, default=None, help="Custom request ID tag")

    # 2. shell
    p_shell = subparsers.add_parser("shell", help="Launch interactive REPL mode")
    p_shell.add_argument(
        "--source", choices=["real", "stand-in"], default="stand-in", help="Initial model backend"
    )
    p_shell.add_argument(
        "--setting",
        choices=["A", "B", "C", "A-degraded"],
        default="A",
        help="Initial retrieval setting",
    )

    # 3. calibrate
    p_cal = subparsers.add_parser("calibrate", help="Run Phase 1 calibration suite")
    p_cal.add_argument(
        "--max-requests", type=int, default=45, help="Maximum deliberate probe requests"
    )
    p_cal.add_argument(
        "--latency-samples", type=int, default=10, help="Number of spaced streaming probes for the latency distribution"
    )

    # 4. eval
    p_eval = subparsers.add_parser("eval", help="Run evaluation rubric sweep across settings")
    p_eval.add_argument(
        "--settings",
        nargs="+",
        default=["A", "B", "C", "A-degraded"],
        help="Retrieval settings to benchmark",
    )
    p_eval.add_argument(
        "--source", choices=["real", "stand-in"], default="real", help="Evaluation backend"
    )
    p_eval.add_argument(
        "--questions",
        type=str,
        default=str(DATA_DIR / "questions.yaml"),
        help="Path to questions rubric file",
    )

    # 5. load
    p_load = subparsers.add_parser("load", help="Execute open-loop load scenarios")
    p_load.add_argument(
        "--scenario",
        choices=["steady", "burst", "overload", "rpm_halved", "budget_exhausted", "all"],
        default="steady",
        help="Load scenario to run",
    )
    p_load.add_argument(
        "--source",
        choices=["real", "stand-in", "from-config"],
        default="from-config",
        help="Model backend override",
    )
    p_load.add_argument(
        "--cooldown",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Enable/disable circuit breaker cooldown on repeat 429s",
    )

    # 6. analyze
    p_an = subparsers.add_parser("analyze", help="Analyze results and produce tables/traces")
    p_an.add_argument("--tables", action="store_true", help="Print Tables 1, 2a, 2b, 3 in Markdown")
    p_an.add_argument("--verify", action="store_true", help="Verify results.csv schema and numbers")
    p_an.add_argument(
        "--waterfall", type=str, metavar="REQUEST_ID", help="Print ASCII latency waterfall for a request"
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    # Windows event loop policy compatibility
    if sys.platform.startswith("win"):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    try:
        asyncio.run(main_async(args))
    except KeyboardInterrupt:
        sys.stderr.write("\nInterrupted by user.\n")
        sys.exit(130)


if __name__ == "__main__":
    main()
