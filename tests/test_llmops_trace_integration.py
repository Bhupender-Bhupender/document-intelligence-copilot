from __future__ import annotations

import mlflow
import pytest

from mlflow.tracking import MlflowClient

from app.serving_service import (
    answer_with_evidence,
)
from src.core.config import config
from src.generation.evidence_answer_engine import (
    generate_from_evidence,
)
from src.retrieval.retrieval_service import (
    run_retrieval_service,
)
from src.schema.serving_models import (
    ServingAnswerRequest,
)


pytest.importorskip("alembic")
pytest.importorskip("sqlalchemy")


def test_real_mlflow_trace_tree_is_safe_and_complete(
    tmp_path,
    monkeypatch,
):
    previous_uri = (
        mlflow.get_tracking_uri()
    )

    tracking_uri = (
        "sqlite:///"
        + (
            tmp_path
            / "mlflow.db"
        ).as_posix()
    )

    try:
        mlflow.set_tracking_uri(
            tracking_uri
        )

        experiment = (
            mlflow.set_experiment(
                "phase15-trace-integration"
            )
        )

        monkeypatch.setattr(
            config,
            "llmops_tracing_enabled",
            True,
        )

        monkeypatch.setattr(
            config,
            "generation_backend",
            "ollama",
        )

        retrieval_times = iter(
            [
                100.0,
                100.010,
            ]
        )

        generation_times = iter(
            [
                200.0,
                200.020,
            ]
        )

        serving_times = iter(
            [
                300.0,
                300.050,
            ]
        )

        def fake_search(
            query,
            top_k,
            filters,
        ):
            assert query == (
                "PRIVATE_QUERY_VALUE"
            )
            return []

        def local_retrieval(request):
            return run_retrieval_service(
                request,
                _retrieve=fake_search,
                _clock=lambda: next(
                    retrieval_times
                ),
            )

        def fake_llm(messages):
            assert messages
            return "PRIVATE_ANSWER_VALUE"

        def local_generation(request):
            return generate_from_evidence(
                request,
                _generator=fake_llm,
                _clock=lambda: next(
                    generation_times
                ),
            )

        response = answer_with_evidence(
            ServingAnswerRequest(
                query="PRIVATE_QUERY_VALUE",
                model="phase15-trace-model",
                top_k=10,
                final_k=3,
            ),
            _retrieval_runner=(
                local_retrieval
            ),
            _generation_runner=(
                local_generation
            ),
            _tracing_enabled=True,
            _clock=lambda: next(
                serving_times
            ),
        )

        client = MlflowClient()

        traces = client.search_traces(
            locations=[
                experiment.experiment_id
            ],
            max_results=10,
            include_spans=True,
            flush=True,
        )

        assert len(traces) == 1

        trace = traces[0]
        spans = list(
            trace.data.spans
        )

        assert len(spans) == 6

        by_name = {
            span.name: span
            for span in spans
        }

        assert set(by_name) == {
            "rag_request",
            "retrieval",
            "evidence_build",
            "generation",
            "prompt_build",
            "llm_call",
        }

        expected_types = {
            "rag_request": "CHAIN",
            "retrieval": "RETRIEVER",
            "evidence_build": "CHAIN",
            "generation": "CHAIN",
            "prompt_build": "CHAIN",
            "llm_call": "LLM",
        }

        for name, expected in (
            expected_types.items()
        ):
            assert (
                by_name[name].span_type
                == expected
            )

        root = by_name["rag_request"]
        retrieval = by_name["retrieval"]
        evidence = by_name["evidence_build"]
        generation = by_name["generation"]
        prompt = by_name["prompt_build"]
        llm = by_name["llm_call"]

        assert root.parent_id is None

        assert (
            retrieval.parent_id
            == root.span_id
        )

        assert (
            evidence.parent_id
            == retrieval.span_id
        )

        assert (
            generation.parent_id
            == root.span_id
        )

        assert (
            prompt.parent_id
            == generation.span_id
        )

        assert (
            llm.parent_id
            == generation.span_id
        )

        for span in spans:
            assert span.inputs is None
            assert span.outputs is None

        serialized = trace.to_json()

        assert (
            "PRIVATE_QUERY_VALUE"
            not in serialized
        )

        assert (
            "PRIVATE_ANSWER_VALUE"
            not in serialized
        )

        assert response.answer_text == (
            "PRIVATE_ANSWER_VALUE"
        )

        assert response.evidence == []
        assert response.sources == []

    finally:
        mlflow.set_tracking_uri(
            previous_uri
        )
