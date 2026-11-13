import asyncio
import csv
import json
import random
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import yaml

try:
    from relay.budget import BudgetLedger
    from relay.config import BASE_DIR, DATA_DIR, RESULTS_DIR
    from relay.groq_client import GroqClient
    from relay.orchestrator import RelayOrchestrator, RequestContext
    from relay.retrieval import RetrievalEngine
except ImportError:
    from src.relay.budget import BudgetLedger
    from src.relay.config import BASE_DIR, DATA_DIR, RESULTS_DIR
    from src.relay.groq_client import GroqClient
    from src.relay.orchestrator import RelayOrchestrator, RequestContext
    from src.relay.retrieval import RetrievalEngine

SCENARIOS_FILE = BASE_DIR / "scenarios.yaml"
QUESTIONS_FILE = DATA_DIR / "questions.yaml"
RESULTS_FILE = RESULTS_DIR / "results.csv"
RUNS_DIR = RESULTS_DIR / "runs"


class OpenLoopLoadGenerator:
    def __init__(
        self,
        orchestrator: RelayOrchestrator,
        scenarios_file: Path = SCENARIOS_FILE,
        questions_file: Path = QUESTIONS_FILE,
        results_file: Path = RESULTS_FILE,
    ) -> None:
        self.orchestrator = orchestrator
        self.scenarios_file = scenarios_file
        self.questions_file = questions_file
        self.results_file = results_file
        self.questions = self._load_questions()

    def _load_questions(self) -> List[Dict]:
        if not self.questions_file.exists():
            return [{"id": "fallback-1", "question": "What is the Transformer architecture?"}]
        with open(self.questions_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return data.get("questions", [])

    def _append_result_row(self, ctx: RequestContext) -> None:
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
            ctx.answer_grade if ctx.answer_grade else "",
        ]
        with open(self.results_file, "a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            writer.writerow(row)

    async def run_scenario(
        self,
        scenario_name: str,
        retrieval_setting: str = "A",
        override_count: Optional[int] = None,
        override_rate_rpm: Optional[float] = None,
    ) -> List[RequestContext]:
        with open(self.scenarios_file, "r", encoding="utf-8") as f:
            scenarios = yaml.safe_load(f).get("scenarios", {})

        cfg = scenarios.get(
            scenario_name,
            {
                "offered_rate_rpm": 3.0,
                "duration_s": 60,
                "requests_count": 5,
                "source": self.orchestrator.client.source_name,
            },
        )

        offered_rpm = override_rate_rpm or cfg.get("offered_rate_rpm", 3.0)
        requests_count = override_count or cfg.get("requests_count", 5)
        interval_s = 60.0 / offered_rpm if offered_rpm > 0 else 1.0

        run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        run_dir = RUNS_DIR / f"{scenario_name}_{run_timestamp}"
        run_dir.mkdir(parents=True, exist_ok=True)

        print(
            f"=== Launching scenario '{scenario_name}': {requests_count} requests @ {offered_rpm} RPM (interval {interval_s:.2f}s) ==="
        )

        sent_count = 0
        completed_count = 0
        inflight_count = 0
        rate_limited_count = 0
        timeout_count = 0

        tasks = []
        contexts: List[RequestContext] = []
        lock = asyncio.Lock()

        def print_ticker():
            sys.stderr.write(
                f"\r[loadgen] Sent: {sent_count}/{requests_count} | Completed: {completed_count} | In-flight: {inflight_count} | 429s: {rate_limited_count} | Timeouts: {timeout_count}"
            )
            sys.stderr.flush()

        async def worker(req_idx: int, q_item: Dict):
            nonlocal completed_count, inflight_count, rate_limited_count, timeout_count
            req_id = f"{scenario_name}-{req_idx:04d}"
            ctx = await self.orchestrator.handle_request(
                request_id=req_id,
                question=q_item["question"],
                scenario=scenario_name,
                retrieval_setting=retrieval_setting,
            )
            self._append_result_row(ctx)

            async with lock:
                contexts.append(ctx)
                completed_count += 1
                inflight_count = max(0, inflight_count - 1)
                if ctx.status == "rate_limited":
                    rate_limited_count += 1
                elif ctx.status in ("timeout", "error"):
                    timeout_count += 1
                print_ticker()

            return ctx

        # Open-loop timer wheel firing
        start_time = time.time()
        for idx in range(requests_count):
            target_fire_time = start_time + (idx * interval_s)
            now = time.time()
            sleep_needed = target_fire_time - now
            if sleep_needed > 0:
                await asyncio.sleep(sleep_needed)

            q_item = random.choice(self.questions)
            async with lock:
                sent_count += 1
                inflight_count += 1
                print_ticker()

            t = asyncio.create_task(worker(idx + 1, q_item))
            tasks.append(t)

        await asyncio.gather(*tasks, return_exceptions=True)
        sys.stderr.write("\n")
        sys.stderr.flush()

        # Archive run artifacts
        with open(run_dir / "config.json", "w", encoding="utf-8") as f:
            json.dump(
                {
                    "scenario": scenario_name,
                    "offered_rpm": offered_rpm,
                    "requests_count": requests_count,
                    "retrieval_setting": retrieval_setting,
                    "source": self.orchestrator.client.source_name,
                },
                f,
                indent=2,
            )

        print(f"Scenario '{scenario_name}' completed. Processed {len(contexts)} requests.")
        return contexts
