from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.evaluation.canonical_dataset import (
    CanonicalEvaluationDataset,
    load_canonical_evaluation_dataset,
)
from src.evaluation.canonical_retrieval_dataset import (
    load_databricks_retrieval_examples,
)
from src.llmops.versioning import (
    LLMOpsVersionContext,
    build_version_context,
)
from src.schema.eval_models import (
    EvalExample,
)
from src.schema.retrieval_eval_models import (
    RetrievalEvalExample,
)


@dataclass(frozen=True)
class EvaluationDatasetBundle:
    """
    One validated evaluation dataset projected
    into the existing evaluation contracts.
    """

    dataset: CanonicalEvaluationDataset

    eval_examples: tuple[
        EvalExample,
        ...,
    ]

    retrieval_examples: tuple[
        RetrievalEvalExample,
        ...,
    ]

    @property
    def evaluation_dataset_version(
        self,
    ) -> str:
        return (
            self.dataset
            .evaluation_dataset_version
        )

    @property
    def active_case_count(
        self,
    ) -> int:
        return (
            self.dataset
            .active_case_count
        )

    def safe_metadata(
        self,
    ) -> dict[
        str,
        str | int | bool,
    ]:
        """
        Return metadata safe for MLflow tracking.

        Queries, expected document identities,
        and document content are excluded.
        """
        metadata = dict(
            self.dataset.safe_metadata()
        )

        metadata.update(
            {
                "eval_example_count":
                    len(
                        self.eval_examples
                    ),
                "retrieval_example_count":
                    len(
                        self.retrieval_examples
                    ),
            }
        )

        return metadata


def load_evaluation_dataset_bundle(
    canonical_path: Path,
    corpus_manifest_path: Path,
) -> EvaluationDatasetBundle:
    """
    Load one canonical dataset and project it
    into deterministic/semantic and retrieval
    evaluation contracts.

    Case identity and ordering must match
    exactly across both projections.
    """
    dataset = (
        load_canonical_evaluation_dataset(
            canonical_path
        )
    )

    eval_examples = tuple(
        dataset.to_eval_examples()
    )

    retrieval_examples = tuple(
        load_databricks_retrieval_examples(
            canonical_path,
            corpus_manifest_path,
        )
    )

    canonical_case_ids = tuple(
        case.case_id
        for case in dataset.active_cases
    )

    eval_case_ids = tuple(
        example.example_id
        for example in eval_examples
    )

    retrieval_case_ids = tuple(
        example.case_id
        for example in retrieval_examples
    )

    if (
        eval_case_ids
        != canonical_case_ids
    ):
        raise ValueError(
            "Deterministic evaluation projection "
            "is not aligned with canonical cases."
        )

    if (
        retrieval_case_ids
        != canonical_case_ids
    ):
        raise ValueError(
            "Retrieval evaluation projection "
            "is not aligned with canonical cases."
        )

    return EvaluationDatasetBundle(
        dataset=dataset,
        eval_examples=eval_examples,
        retrieval_examples=(
            retrieval_examples
        ),
    )


def build_version_context_for_dataset(
    bundle: EvaluationDatasetBundle,
    *,
    generation_model: str,
    embedding_model: str,
    index_name: str,
    code_revision: str | None = None,
    repo_root: Path | None = None,
) -> LLMOpsVersionContext:
    """
    Build the existing LLMOps version identity
    directly from the validated dataset bundle.
    """
    return build_version_context(
        generation_model=(
            generation_model
        ),
        embedding_model=(
            embedding_model
        ),
        index_name=index_name,
        evaluation_dataset_version=(
            bundle
            .evaluation_dataset_version
        ),
        code_revision=code_revision,
        repo_root=repo_root,
    )