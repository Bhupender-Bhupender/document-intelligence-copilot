from pathlib import Path

import pytest

from src.llmops.versioning import (
    CHUNKING_CONTRACT_VERSION,
    EVALUATION_CONTRACT_VERSION,
    PROMPT_CONTRACT_VERSION,
    build_version_context,
    resolve_code_revision,
)
from src.schema.retrieval_service_models import RETRIEVAL_CONFIG_VERSION


def test_build_version_context_reuses_retrieval_contract():
    context = build_version_context(
        generation_model="generation-model",
        embedding_model="embedding-model",
        index_name="catalog.schema.index",
        evaluation_dataset_version="evaluation_cases_v1",
        code_revision="abc123",
    )

    assert (
        context.retrieval_config_version
        == RETRIEVAL_CONFIG_VERSION
    )
    assert (
        context.prompt_contract_version
        == PROMPT_CONTRACT_VERSION
    )
    assert (
        context.chunking_contract_version
        == CHUNKING_CONTRACT_VERSION
    )
    assert (
        context.evaluation_contract_version
        == EVALUATION_CONTRACT_VERSION
    )
    assert context.code_revision == "abc123"


def test_version_context_tags_are_strings():
    context = build_version_context(
        generation_model="generation-model",
        embedding_model="embedding-model",
        index_name="catalog.schema.index",
        evaluation_dataset_version="evaluation_cases_v1",
        code_revision="abc123",
    )

    tags = context.as_tags()

    assert tags
    assert all(
        isinstance(key, str)
        and isinstance(value, str)
        for key, value in tags.items()
    )


@pytest.mark.parametrize(
    "field_name",
    [
        "generation_model",
        "embedding_model",
        "index_name",
        "evaluation_dataset_version",
    ],
)
def test_required_version_identity_rejects_blank_values(
    field_name: str,
):
    kwargs = {
        "generation_model": "generation-model",
        "embedding_model": "embedding-model",
        "index_name": "catalog.schema.index",
        "evaluation_dataset_version": "evaluation_cases_v1",
        "code_revision": "abc123",
    }
    kwargs[field_name] = "   "

    with pytest.raises(
        ValueError,
        match=field_name,
    ):
        build_version_context(**kwargs)


def test_resolve_code_revision_prefers_environment():
    revision = resolve_code_revision(
        env={
            "GIT_COMMIT": "environment-sha",
        }
    )

    assert revision == "environment-sha"


def test_resolve_code_revision_uses_git_repo():
    revision = resolve_code_revision(
        repo_root=Path.cwd(),
        env={},
    )

    assert revision != "unknown"
    assert len(revision) >= 7
