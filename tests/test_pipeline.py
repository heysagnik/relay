import asyncio
import json
import pytest
from pathlib import Path

from src.relay.budget import BudgetLedger
from src.relay.config import MODEL_DRAFT, MODEL_TRIAGE
from src.relay.groq_client import GroqClient, StreamChunk
from src.relay.orchestrator import RelayOrchestrator
from src.relay.retrieval import RetrievalEngine


class MockLLMClient:
    def __init__(self):
        self.source_name = "mock"

    async def stream_chat_completion(
        self,
        model: str,
        messages: list,
        temperature: float = 0.0,
        max_tokens: int = 1024,
        max_retries: int = 2,
        response_format=None,
        on_retry=None,
        lease_id=None,
    ):
        prompt = messages[-1]["content"]
        if "Classify this question" in prompt:
            # Stage 1 Triage mock response
            content = json.dumps(
                {"classification": "answerable", "target_paper": "AISNY", "queries": ["query"]}
            )
            yield StreamChunk(delta_text=content, tokens_in=50, tokens_out=30, first_token=True)
        else:
            # Stage 4 Draft mock: cite a tag from a passage actually provided in
            # the prompt (as a real grounded model would) — the S5 verifier now
            # requires exact tag membership.
            import re as _re
            m = _re.search(r"Passage \((\[[^\]]+\])\):", prompt)
            tag = m.group(1) if m else "[AISNY §2, p.1]"
            words = [
                "The", "base", "Transformer", "model", "uses", "d_model", "=", "512",
                "for", "input", "embeddings", f"{tag}", "This", "enables",
                "effective", "representation", f"{tag}"
            ]
            first = True
            for w in words:
                await asyncio.sleep(0.01)
                yield StreamChunk(delta_text=w + " ", tokens_in=300, tokens_out=25, first_token=first)
                first = False


def test_end_to_end_pipeline(tmp_path):
    async def _run():
        budget_file = tmp_path / "budget.json"
        ledger = BudgetLedger(path=budget_file)
        retrieval = RetrievalEngine()
        client = MockLLMClient()

        orchestrator = RelayOrchestrator(
            llm_client=client,
            retrieval_engine=retrieval,
            budget_ledger=ledger,
        )

        ctx = await orchestrator.handle_request(
            request_id="test-0001",
            question="What is the dimension of the base Transformer?",
            scenario="steady",
            retrieval_setting="A",
        )

        assert ctx.status == "ok"
        assert ctx.first_token_ms is not None
        assert ctx.first_token_ms > 0
        assert "[AISNY" in ctx.final_answer
        assert ctx.tokens_in > 0
        assert ctx.tokens_out > 0

    asyncio.run(_run())


def test_unanswerable_refusal(tmp_path):
    async def _run():
        class RefusalTriageClient:
            def __init__(self):
                self.source_name = "mock"

            async def stream_chat_completion(self, *args, **kwargs):
                content = json.dumps(
                    {"classification": "out_of_scope", "target_paper": "None", "queries": []}
                )
                yield StreamChunk(delta_text=content, tokens_in=50, tokens_out=25, first_token=True)

        ledger = BudgetLedger(path=tmp_path / "budget.json")
        retrieval = RetrievalEngine()
        client = RefusalTriageClient()

        orchestrator = RelayOrchestrator(
            llm_client=client,
            retrieval_engine=retrieval,
            budget_ledger=ledger,
        )

        ctx = await orchestrator.handle_request(
            request_id="test-refuse-0001",
            question="What is the training cost of GPT-4 in dollars?",
            scenario="steady",
            retrieval_setting="A",
        )

        assert ctx.status == "ok"
        assert ctx.answer_grade == "refused_correctly"
        assert "Refusal:" in ctx.final_answer

    asyncio.run(_run())


def test_fabricated_citation_is_rejected(tmp_path):
    """A citation to a section/page that was never retrieved must fail S5
    verification even when a passage from the same paper was retrieved."""
    async def _run():
        class FabricatingClient:
            source_name = "mock"

            async def stream_chat_completion(self, model, messages, temperature=0.0,
                                             max_tokens=1024, max_retries=2,
                                             response_format=None, on_retry=None,
                                             lease_id=None):
                prompt = messages[-1]["content"]
                if "Classify this question" in prompt:
                    content = json.dumps(
                        {"classification": "answerable", "target_paper": "AISNY", "queries": ["q"]}
                    )
                    yield StreamChunk(delta_text=content, tokens_in=50, tokens_out=30, first_token=True)
                else:
                    # Fabricated: same paper, but a section/page that was never retrieved.
                    for w in ["The", "model", "uses", "d_model", "=", "512",
                              "[AISNY §99, p.999]."]:
                        await asyncio.sleep(0.01)
                        yield StreamChunk(delta_text=w + " ", tokens_in=300, tokens_out=25, first_token=False)

        ledger = BudgetLedger(path=tmp_path / "budget.json")
        orch = RelayOrchestrator(
            llm_client=FabricatingClient(),
            retrieval_engine=RetrievalEngine(),
            budget_ledger=ledger,
        )
        ctx = await orch.handle_request(
            request_id="test-fabricated-0001",
            question="What is the dimension of the base Transformer?",
            scenario="steady",
            retrieval_setting="A",
        )
        assert ctx.status == "ok"
        assert ctx.answer_grade == "refused_correctly"
        assert ctx.refusal_reason == "no_verified_answer"

    asyncio.run(_run())
