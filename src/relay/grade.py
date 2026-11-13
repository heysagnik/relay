import asyncio
import csv
import json
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml

try:
    from relay.analyze import compute_table_3, load_results_rows
    from relay.config import DATA_DIR, RESULTS_DIR
    from relay.orchestrator import RelayOrchestrator, RequestContext
except ImportError:
    from src.relay.analyze import compute_table_3, load_results_rows
    from src.relay.config import DATA_DIR, RESULTS_DIR
    from src.relay.orchestrator import RelayOrchestrator, RequestContext

QUESTIONS_FILE = DATA_DIR / "questions.yaml"
RESULTS_CSV = RESULTS_DIR / "results.csv"
RUNS_DIR = RESULTS_DIR / "runs"


@dataclass
class EvaluationResult:
    question_id: str
    grade: str
    has_valid_citation: bool
    gold_overlap: float
    notes: str


class AnswerEvaluator:
    def __init__(self, questions_file: Path = QUESTIONS_FILE) -> None:
        self.questions_file = questions_file
        self.questions: Dict[str, Dict] = self._load_questions()

    def _load_questions(self) -> Dict[str, Dict]:
        if not self.questions_file.exists():
            return {}
        with open(self.questions_file, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
            return {q["id"]: q for q in data.get("questions", [])}

    def evaluate_answer(
        self, question_id: str, answer_text: str, status: str
    ) -> EvaluationResult:
        q_meta = self.questions.get(question_id)
        if status != "ok":
            # Availability failure (rate_limited / timeout / error): not an answer,
            # so it carries no quality grade. Availability is reported via the
            # status column and Table 2, not Table 3.
            return EvaluationResult(
                question_id=question_id,
                grade="",
                has_valid_citation=False,
                gold_overlap=0.0,
                notes=f"No quality grade: status={status}",
            )
        if not q_meta:
            return EvaluationResult(
                question_id=question_id,
                grade="wrong" if status == "error" else "correct",
                has_valid_citation=False,
                gold_overlap=0.0,
                notes="Question ID not in rubric",
            )

        is_unanswerable = (
            "unanswerable" in question_id
            or q_meta.get("expected_source") == "None"
            or "UNANSWERABLE" in q_meta.get("gold_answer_summary", "")
        )

        lower_ans = answer_text.lower()
        is_refusal = (
            "refusal:" in lower_ans
            or "cannot answer" in lower_ans
            or "cannot cite" in lower_ans
            or "does not state" in lower_ans
            or "does not specify" in lower_ans
            or "does not report" in lower_ans
            or "not disclosed" in lower_ans
            or "not stated" in lower_ans
            or "do not state" in lower_ans
            or "ambiguous" in lower_ans
            or "out of scope" in lower_ans
            or "out-of-scope" in lower_ans
        )

        citations = re.findall(r"\[(AISNY|Moshi|DynChunk)\s+§[0-9.]*,\s*p\.[0-9]+\]", answer_text)

        if is_unanswerable:
            if is_refusal:
                return EvaluationResult(
                    question_id=question_id,
                    grade="refused_correctly",
                    has_valid_citation=False,
                    gold_overlap=1.0,
                    notes="Unanswerable question properly refused",
                )
            else:
                return EvaluationResult(
                    question_id=question_id,
                    grade="wrong",
                    has_valid_citation=bool(citations),
                    gold_overlap=0.0,
                    notes="Hallucinated answer for unanswerable question",
                )

        if is_refusal:
            return EvaluationResult(
                question_id=question_id,
                grade="refused_wrongly",
                has_valid_citation=False,
                gold_overlap=0.0,
                notes="Answerable question was improperly refused",
            )

        expected_paper = q_meta.get("paper", "")
        has_expected_citation = any(
            expected_paper in c for c in citations
        ) if expected_paper != "ALL" else bool(citations)

        # Lexical overlap with gold answer summary
        gold_words = set(re.findall(r"\b\w{3,}\b", q_meta.get("gold_answer_summary", "").lower()))
        ans_words = set(re.findall(r"\b\w{3,}\b", lower_ans))
        overlap = len(gold_words & ans_words) / max(1, len(gold_words))

        grade = "correct" if (overlap >= 0.20 and has_expected_citation) else "wrong"
        notes = f"Overlap: {overlap:.2f}, HasCitation: {has_expected_citation}"

        return EvaluationResult(
            question_id=question_id,
            grade=grade,
            has_valid_citation=has_expected_citation,
            gold_overlap=overlap,
            notes=notes,
        )


async def run_evaluation_sweep(
    orchestrator: RelayOrchestrator,
    settings: List[str],
    questions_file: Path = QUESTIONS_FILE,
) -> None:
    evaluator = AnswerEvaluator(questions_file=questions_file)
    with open(questions_file, "r", encoding="utf-8") as f:
        questions = yaml.safe_load(f).get("questions", [])

    run_timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    run_dir = RUNS_DIR / f"eval_sweep_{run_timestamp}"
    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"=== Starting Evaluation Sweep over settings: {settings} ({len(questions)} questions each) ===")

    new_rows = []
    for s in settings:
        print(f"\n--- Running Setting {s} ---")
        for q in questions:
            q_id = q["id"]
            req_id = f"eval-{s}-{q_id}"
            print(f"Evaluating {req_id}: {q['question'][:50]}...", end=" ", flush=True)

            ctx = await orchestrator.handle_request(
                request_id=req_id,
                question=q["question"],
                scenario="eval",  # distinct scenario so eval rows never pollute Table 2 load scenarios
                retrieval_setting=s,
                allow_lease_degrade=False,  # quality sweep: keep the requested setting
                use_requested_setting=True,
            )

            res = evaluator.evaluate_answer(q_id, ctx.final_answer, ctx.status)
            ctx.answer_grade = res.grade or None
            print(f"[{res.grade or ctx.status}] (TTFT: {ctx.first_token_ms}ms, Total: {ctx.total_ms}ms)")

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
            new_rows.append(row)

            # Fixed spacing keeps the per-minute token window under the lease
            # headroom (≈2.9K est/question vs 7.2K usable), so measurements
            # contain no throttle waits and no 429s — pure setting cost.
            await asyncio.sleep(30.0)

    with open(run_dir / "eval_metadata.json", "w", encoding="utf-8") as f:
        json.dump(
            {
                "timestamp": run_timestamp,
                "settings": settings,
                "questions_count": len(questions),
                "source": orchestrator.client.source_name,
            },
            f,
            indent=2,
        )

    print("\nEvaluation sweep complete! Recomputing Table 3:")
    all_rows = load_results_rows()
    print(compute_table_3(all_rows))
