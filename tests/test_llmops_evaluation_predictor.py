from __future__ import annotations

import csv
import json

from pathlib import Path

import pytest

from src.llmops.deterministic_scorers import (
    answer_non_empty,
    citation_present,
    evidence_present,
    expected_document_hit,
)
from src.llmops.evaluation_dataset import (
    load_evaluation_dataset_bundle,
)
from src.llmops.evaluation_predictor import (
    build_case_query_lookup,
    make_serving_evaluation_predict_fn,
)
from src.schema.retrieval_service_models import (
    RetrievalEvidence,
)
from src.schema.serving_models import (
    ServingAnswerResponse,
)


_PRIVATE_QUERY = (
    "PRIVATE_CANONICAL_QUERY"
)


def _score_value(
    result,
):
    return getattr(
        result,
        "value",
        result,
    )


def _bundle(
    tmp_path: Path,
):
    canonical_path = (
        tmp_path
        / "evaluation_cases_v1.jsonl"
    )

    manifest_path = (
        tmp_path
        / "manifest.csv"
    )

    canonical_path.write_text(
        json.dumps(
            {
                "case_id":
                    "case-1",
                "dataset_id":
                    "source-a",
                "version":
                    "1.0",
                "query":
                    _PRIVATE_QUERY,
                "target_document_id":
                    "baseline-doc-1",
                "is_active":
                    True,
                "comment":
                    "",
            }
        )
        + "\n",
        encoding="utf-8",
    )

    with manifest_path.open(
        "w",
        encoding="utf-8",
        newline="",
    ) as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "document_id",
                "sha256",
            ],
        )

        writer.writeheader()

        writer.writerow(
            {
                "document_id":
                    "baseline-doc-1",
                "sha256":
                    "a" * 64,
            }
        )

    return (
        load_evaluation_dataset_bundle(
            canonical_path,
            manifest_path,
        )
    )


def _serving_response():
    return (
        ServingAnswerResponse.model_construct(
            answer_text=(
                "Grounded answer."
            ),
            evidence=[
                RetrievalEvidence.model_construct(
                    document_id=(
                        "doc_aaaaaaaaaaaaaaaa"
                    ),
                )
            ],
            sources=[
                object(),
            ],
        )
    )


def test_lookup_resolves_canonical_query(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    lookup = build_case_query_lookup(
        bundle
    )

    assert lookup == {
        "case-1":
            _PRIVATE_QUERY,
    }


def test_predictor_forwards_private_query_only_to_serving(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    seen = []

    def runner(request):
        seen.append(
            request.query
        )

        return _serving_response()

    predict_fn = (
        make_serving_evaluation_predict_fn(
            bundle,
            _serving_runner=runner,
        )
    )

    output = predict_fn(
        case_id="case-1"
    )

    assert seen == [
        _PRIVATE_QUERY,
    ]

    assert (
        _PRIVATE_QUERY
        not in repr(output)
    )


def test_predictor_returns_normalized_contract(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    predict_fn = (
        make_serving_evaluation_predict_fn(
            bundle,
            _serving_runner=(
                lambda request:
                    _serving_response()
            ),
        )
    )

    output = predict_fn(
        case_id="case-1"
    )

    assert output == {
        "answer_text":
            "Grounded answer.",

        "retrieved_document_ids":
            [
                "doc_aaaaaaaaaaaaaaaa",
            ],

        "evidence_count":
            1,

        "citation_count":
            1,
    }


def test_predictor_rejects_unknown_case(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    predict_fn = (
        make_serving_evaluation_predict_fn(
            bundle,
            _serving_runner=(
                lambda request:
                    _serving_response()
            ),
        )
    )

    with pytest.raises(
        ValueError,
        match="Unknown evaluation case",
    ):
        predict_fn(
            case_id="missing"
        )


def test_predictor_requires_serving_response(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    predict_fn = (
        make_serving_evaluation_predict_fn(
            bundle,
            _serving_runner=(
                lambda request:
                    {"answer_text": "wrong"}
            ),
        )
    )

    with pytest.raises(
        TypeError,
        match="ServingAnswerResponse",
    ):
        predict_fn(
            case_id="case-1"
        )


def test_predictor_output_passes_all_scorers(
    tmp_path,
):
    bundle = _bundle(
        tmp_path
    )

    predict_fn = (
        make_serving_evaluation_predict_fn(
            bundle,
            _serving_runner=(
                lambda request:
                    _serving_response()
            ),
        )
    )

    output = predict_fn(
        case_id="case-1"
    )

    expectations = {
        "expected_document_id":
            "doc_aaaaaaaaaaaaaaaa",

        "expect_non_empty_answer":
            True,
    }

    results = [
        answer_non_empty(
            outputs=output,
            expectations=expectations,
        ),
        expected_document_hit(
            outputs=output,
            expectations=expectations,
        ),
        evidence_present(
            outputs=output,
        ),
        citation_present(
            outputs=output,
        ),
    ]

    assert all(
        _score_value(result)
        is True
        for result in results
    )