from __future__ import annotations

import argparse

from pathlib import Path
from typing import Sequence

from src.llmops.evaluation_dataset import (
    build_version_context_for_dataset,
    load_evaluation_dataset_bundle,
)
from src.llmops.evaluation_workflow import (
    run_serving_mlflow_evaluation,
)
from src.llmops.mlflow_tracking import (
    MLflowExperimentConfig,
)


DEFAULT_CANONICAL_PATH = (
    "data/eval/canonical/"
    "evaluation_cases_v1.jsonl"
)

DEFAULT_CORPUS_MANIFEST_PATH = (
    "docs/baseline/"
    "corpus_manifest.csv"
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Run privacy-safe deterministic "
            "MLflow evaluation through the "
            "Document Intelligence serving "
            "boundary."
        )
    )

    parser.add_argument(
        "--canonical-path",
        default=DEFAULT_CANONICAL_PATH,
    )

    parser.add_argument(
        "--corpus-manifest-path",
        default=(
            DEFAULT_CORPUS_MANIFEST_PATH
        ),
    )

    parser.add_argument(
        "--experiment-name",
        required=True,
    )

    parser.add_argument(
        "--environment",
        required=True,
    )

    parser.add_argument(
        "--generation-model",
        required=True,
    )

    parser.add_argument(
        "--embedding-model",
        required=True,
    )

    parser.add_argument(
        "--index-name",
        required=True,
    )

    parser.add_argument(
        "--run-name",
        default=(
            "phase15-deterministic-evaluation"
        ),
    )

    parser.add_argument(
        "--tracking-uri",
        default=None,
    )

    parser.add_argument(
        "--code-revision",
        default=None,
        help=(
            "Optional explicit code revision. "
            "When omitted, repository revision "
            "resolution is used."
        ),
    )

    parser.add_argument(
        "--repo-root",
        default=None,
    )

    return parser


def main(
    argv: Sequence[str] | None = None,
) -> int:
    args = build_parser().parse_args(
        argv
    )

    canonical_path = Path(
        args.canonical_path
    )

    corpus_manifest_path = Path(
        args.corpus_manifest_path
    )

    if not canonical_path.is_file():
        raise FileNotFoundError(
            "Canonical evaluation dataset "
            "does not exist."
        )

    if not corpus_manifest_path.is_file():
        raise FileNotFoundError(
            "Corpus manifest does not exist."
        )

    repo_root = (
        Path(args.repo_root)
        if args.repo_root
        else None
    )

    bundle = (
        load_evaluation_dataset_bundle(
            canonical_path,
            corpus_manifest_path,
        )
    )

    version_context = (
        build_version_context_for_dataset(
            bundle,
            generation_model=(
                args.generation_model
            ),
            embedding_model=(
                args.embedding_model
            ),
            index_name=args.index_name,
            code_revision=(
                args.code_revision
            ),
            repo_root=repo_root,
        )
    )

    config = MLflowExperimentConfig(
        experiment_name=(
            args.experiment_name
        ),
        environment=args.environment,
        tracking_uri=(
            args.tracking_uri
        ),
    )

    result = (
        run_serving_mlflow_evaluation(
            bundle,
            config=config,
            version_context=(
                version_context
            ),
            run_name=args.run_name,
        )
    )

    print(
        "MLFLOW_RUN_ID:",
        result.run_id,
    )

    print(
        "EVALUATION_DATASET_VERSION:",
        result.evaluation_dataset_version,
    )

    print(
        "EVALUATED_CASE_COUNT:",
        result.evaluated_case_count,
    )

    print(
        "AGGREGATE_METRICS:"
    )

    for name in sorted(
        result.metrics
    ):
        print(
            " ",
            name,
            "=",
            result.metrics[name],
        )

    return 0


if __name__ == "__main__":
    raise SystemExit(
        main()
    )