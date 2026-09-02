from __future__ import annotations

import os
import subprocess
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Mapping

from src.schema.retrieval_service_models import RETRIEVAL_CONFIG_VERSION


PROMPT_CONTRACT_VERSION = "evidence-grounded-v1"
CHUNKING_CONTRACT_VERSION = "hierarchical-parent-child-v1"
EVALUATION_CONTRACT_VERSION = "project-native-evaluation-v1"


def _required_text(name: str, value: str) -> str:
    resolved = str(value).strip()
    if not resolved:
        raise ValueError(f"{name} must be non-empty.")
    return resolved


def resolve_code_revision(
    repo_root: Path | None = None,
    *,
    env: Mapping[str, str] | None = None,
) -> str:
    """Resolve a non-sensitive source revision for experiment traceability."""

    source = os.environ if env is None else env

    for key in (
        "GIT_COMMIT",
        "GIT_SHA",
        "DATABRICKS_GIT_COMMIT",
        "COMMIT_SHA",
    ):
        value = str(source.get(key, "")).strip()
        if value:
            return value

    try:
        completed = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=True,
            timeout=3,
        )
    except (
        OSError,
        subprocess.SubprocessError,
    ):
        return "unknown"

    value = completed.stdout.strip()
    return value or "unknown"


@dataclass(frozen=True)
class LLMOpsVersionContext:
    """Version identity attached to an evaluation or traced RAG run."""

    retrieval_config_version: str
    prompt_contract_version: str
    chunking_contract_version: str
    evaluation_contract_version: str
    generation_model: str
    embedding_model: str
    index_name: str
    evaluation_dataset_version: str
    code_revision: str

    def as_tags(self) -> dict[str, str]:
        """Return MLflow-safe string tags without importing MLflow."""

        return {
            key: str(value)
            for key, value in asdict(self).items()
        }


def build_version_context(
    *,
    generation_model: str,
    embedding_model: str,
    index_name: str,
    evaluation_dataset_version: str,
    code_revision: str | None = None,
    repo_root: Path | None = None,
) -> LLMOpsVersionContext:
    """Build the canonical Phase 15 version identity."""

    return LLMOpsVersionContext(
        retrieval_config_version=_required_text(
            "retrieval_config_version",
            RETRIEVAL_CONFIG_VERSION,
        ),
        prompt_contract_version=PROMPT_CONTRACT_VERSION,
        chunking_contract_version=CHUNKING_CONTRACT_VERSION,
        evaluation_contract_version=EVALUATION_CONTRACT_VERSION,
        generation_model=_required_text(
            "generation_model",
            generation_model,
        ),
        embedding_model=_required_text(
            "embedding_model",
            embedding_model,
        ),
        index_name=_required_text(
            "index_name",
            index_name,
        ),
        evaluation_dataset_version=_required_text(
            "evaluation_dataset_version",
            evaluation_dataset_version,
        ),
        code_revision=(
            _required_text(
                "code_revision",
                code_revision,
            )
            if code_revision is not None
            else resolve_code_revision(
                repo_root=repo_root,
            )
        ),
    )
