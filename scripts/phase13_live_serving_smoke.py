"""
Phase 13E live deployed-serving smoke test.

Validates:

local authenticated client
    -> deployed Databricks App
    -> FastAPI
    -> Phase 13 serving service
    -> Phase 11 Databricks retrieval
    -> Phase 12 managed generation
    -> citation-ready HTTP response

Private document/query/answer/credential values are never printed.
"""

from __future__ import annotations

import os
from typing import Any

import requests
from databricks.sdk import WorkspaceClient

from scripts.evaluation.run_databricks_retrieval_baseline import (
    CANONICAL_PATH,
    CORPUS_MANIFEST_PATH,
)
from src.evaluation.canonical_retrieval_dataset import (
    load_databricks_retrieval_examples,
)


APP_NAME = os.environ.get(
    "DOCINTEL_APP_NAME",
    "docintel-copilot",
).strip()

PROFILE = os.environ.get(
    "DATABRICKS_CONFIG_PROFILE",
    "docintel-free",
).strip()


def _document_ids(
    example: Any,
) -> list[str]:
    plural = getattr(
        example,
        "expected_document_ids",
        None,
    )

    if plural:
        return list(plural)

    singular = getattr(
        example,
        "expected_document_id",
        None,
    )

    if singular:
        return [singular]

    raise RuntimeError(
        "Canonical example has no "
        "expected document identifier."
    )


def _normalize_app_url(
    value: str,
) -> str:
    value = value.strip().rstrip("/")

    if not value:
        raise RuntimeError(
            "Databricks App URL is unavailable."
        )

    if not value.startswith(
        ("http://", "https://")
    ):
        value = "https://" + value

    return value


def _request_headers(
    client: WorkspaceClient,
) -> dict[str, str]:
    """
    Obtain current user OAuth headers through
    Databricks unified authentication.
    """
    headers = dict(
        client.config.authenticate()
    )

    headers["Content-Type"] = (
        "application/json"
    )

    return headers


def main() -> None:
    if not APP_NAME:
        raise RuntimeError(
            "DOCINTEL_APP_NAME is missing."
        )

    client = WorkspaceClient(
        profile=PROFILE,
    )

    app = client.apps.get(
        APP_NAME
    )

    app_url = _normalize_app_url(
        app.url or ""
    )

    headers = _request_headers(
        client
    )

    # ---------------------------------------------------------
    # 1. Liveness
    # ---------------------------------------------------------
    health_response = requests.get(
        f"{app_url}/api/v1/health",
        headers=headers,
        timeout=30,
    )

    health_http_ok = (
        health_response.status_code == 200
    )

    health_body = (
        health_response.json()
        if health_http_ok
        else {}
    )

    health_contract_ok = (
        health_body.get("status")
        == "ok"
    )

    print(
        "PHASE13E_HEALTH_HTTP_OK:",
        health_http_ok,
    )

    print(
        "PHASE13E_HEALTH_CONTRACT_OK:",
        health_contract_ok,
    )

    if not (
        health_http_ok
        and health_contract_ok
    ):
        raise RuntimeError(
            "Deployed App health check failed."
        )

    # ---------------------------------------------------------
    # 2. Configuration readiness
    # ---------------------------------------------------------
    ready_response = requests.get(
        f"{app_url}/api/v1/ready",
        headers=headers,
        timeout=30,
    )

    ready_body = {}

    try:
        ready_body = ready_response.json()
    except Exception:
        pass

    ready_http_ok = (
        ready_response.status_code == 200
    )

    ready_contract_ok = (
        ready_body.get("status")
        == "ready"
    )

    checks = ready_body.get(
        "checks",
        {},
    )

    readiness_checks_ok = (
        bool(checks)
        and all(
            bool(value)
            for value in checks.values()
        )
    )

    print(
        "PHASE13E_READY_HTTP_OK:",
        ready_http_ok,
    )

    print(
        "PHASE13E_READY_CONTRACT_OK:",
        ready_contract_ok,
    )

    print(
        "PHASE13E_READY_CHECKS_OK:",
        readiness_checks_ok,
    )

    if not (
        ready_http_ok
        and ready_contract_ok
        and readiness_checks_ok
    ):
        failed_names = [
            name
            for name, value
            in checks.items()
            if not value
        ]

        print(
            "PHASE13E_FAILED_READY_CHECK_COUNT:",
            len(failed_names),
        )

        for name in failed_names:
            print(
                "PHASE13E_FAILED_READY_CHECK:",
                name,
            )

        raise RuntimeError(
            "Deployed App is not ready."
        )

    # ---------------------------------------------------------
    # 3. Use one existing canonical retrieval case.
    # ---------------------------------------------------------
    examples = (
        load_databricks_retrieval_examples(
            CANONICAL_PATH,
            CORPUS_MANIFEST_PATH,
        )
    )

    if not examples:
        raise RuntimeError(
            "Canonical evaluation dataset "
            "is empty."
        )

    example = examples[0]

    allowed_document_ids = (
        _document_ids(example)
    )

    payload = {
        "query": example.query,
        "document_ids":
            allowed_document_ids,
        "top_k": 10,
        "final_k": 3,
        "include_parent_context":
            True,
    }

    # ---------------------------------------------------------
    # 4. Full deployed /answer call.
    # ---------------------------------------------------------
    answer_response = requests.post(
        f"{app_url}/api/v1/answer",
        headers=headers,
        json=payload,
        timeout=180,
    )

    answer_http_ok = (
        answer_response.status_code
        == 200
    )

    print(
        "PHASE13E_ANSWER_HTTP_OK:",
        answer_http_ok,
    )

    if not answer_http_ok:
        print(
            "PHASE13E_ANSWER_STATUS_CODE:",
            answer_response.status_code,
        )

        raise RuntimeError(
            "Deployed answer request failed. "
            "Inspect Databricks App logs."
        )

    body = answer_response.json()

    answer_nonempty = bool(
        str(
            body.get(
                "answer_text",
                "",
            )
        ).strip()
    )

    backend_ok = (
        body.get(
            "generation_backend"
        )
        == "databricks"
    )

    model_used = str(
        body.get(
            "model_used",
            "",
        )
    ).strip()

    managed_model_used = bool(
        model_used
    )

    evidence = body.get(
        "evidence",
        []
    )

    sources = body.get(
        "sources",
        []
    )

    evidence_nonempty = (
        len(evidence) > 0
    )

    sources_nonempty = (
        len(sources) > 0
    )

    source_count_matches = (
        len(sources)
        == len(evidence)
        and len(evidence) > 0
    )

    evidence_authorized = (
        evidence_nonempty
        and all(
            item.get(
                "document_id"
            )
            in allowed_document_ids
            for item in evidence
        )
    )

    citation_payloads = [
        item.get(
            "citation_payload"
        )
        for item in evidence
    ]

    exact_citations_preserved = (
        sources
        == citation_payloads
        and bool(sources)
    )

    parent_context_available = any(
        bool(
            str(
                item.get(
                    "parent_text",
                    "",
                )
                or ""
            ).strip()
        )
        for item in evidence
    )

    retrieval_latency = body.get(
        "retrieval_latency_ms"
    )

    generation_latency = body.get(
        "generation_latency_ms"
    )

    total_latency = body.get(
        "total_latency_ms"
    )

    latencies_valid = all(
        isinstance(
            value,
            (int, float),
        )
        and value >= 0
        for value in (
            retrieval_latency,
            generation_latency,
            total_latency,
        )
    )

    retrieval_contract_present = bool(
        str(
            body.get(
                "retrieval_config_version",
                "",
            )
        ).strip()
    )

    generation_contract_present = bool(
        str(
            body.get(
                "generation_contract_version",
                "",
            )
        ).strip()
    )

    applied_filters = body.get(
        "applied_filters",
        []
    )

    document_filter_recorded = any(
        "document" in str(
            item
        ).lower()
        for item in applied_filters
    )

    print(
        "PHASE13E_ANSWER_NONEMPTY:",
        answer_nonempty,
    )

    print(
        "PHASE13E_MANAGED_BACKEND:",
        backend_ok,
    )

    print(
        "PHASE13E_MANAGED_MODEL_USED:",
        managed_model_used,
    )

    print(
        "PHASE13E_EVIDENCE_COUNT:",
        len(evidence),
    )

    print(
        "PHASE13E_SOURCE_COUNT:",
        len(sources),
    )

    print(
        "PHASE13E_SOURCE_COUNT_MATCHES:",
        source_count_matches,
    )

    print(
        "PHASE13E_EVIDENCE_AUTHORIZED:",
        evidence_authorized,
    )

    print(
        "PHASE13E_EXACT_CITATIONS_PRESERVED:",
        exact_citations_preserved,
    )

    print(
        "PHASE13E_PARENT_CONTEXT_AVAILABLE:",
        parent_context_available,
    )

    print(
        "PHASE13E_DOCUMENT_FILTER_RECORDED:",
        document_filter_recorded,
    )

    print(
        "PHASE13E_LATENCIES_VALID:",
        latencies_valid,
    )

    print(
        "PHASE13E_RETRIEVAL_CONTRACT_PRESENT:",
        retrieval_contract_present,
    )

    print(
        "PHASE13E_GENERATION_CONTRACT_PRESENT:",
        generation_contract_present,
    )

    # Parent context is useful evidence but is not
    # required for the serving contract to be valid.
    phase13e_pass = all(
        [
            health_http_ok,
            health_contract_ok,
            ready_http_ok,
            ready_contract_ok,
            readiness_checks_ok,
            answer_http_ok,
            answer_nonempty,
            backend_ok,
            managed_model_used,
            evidence_nonempty,
            sources_nonempty,
            source_count_matches,
            evidence_authorized,
            exact_citations_preserved,
            document_filter_recorded,
            latencies_valid,
            retrieval_contract_present,
            generation_contract_present,
        ]
    )

    print(
        "PHASE13E_LIVE_SERVING_PASS:",
        phase13e_pass,
    )

    if not phase13e_pass:
        raise RuntimeError(
            "Phase 13E live serving "
            "validation failed."
        )


if __name__ == "__main__":
    main()
