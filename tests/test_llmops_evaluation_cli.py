from __future__ import annotations

import subprocess
import sys

from pathlib import Path

import pytest

from src.llmops.evaluation_cli import (
    DEFAULT_CANONICAL_PATH,
    DEFAULT_CORPUS_MANIFEST_PATH,
    build_parser,
)


def test_cli_defaults_to_canonical_v1():
    parser = build_parser()

    args = parser.parse_args(
        [
            "--experiment-name",
            "experiment",
            "--environment",
            "test",
            "--generation-model",
            "generation",
            "--embedding-model",
            "embedding",
            "--index-name",
            "index",
        ]
    )

    assert (
        args.canonical_path
        == DEFAULT_CANONICAL_PATH
    )

    assert (
        args.corpus_manifest_path
        == DEFAULT_CORPUS_MANIFEST_PATH
    )


@pytest.mark.parametrize(
    "missing_option",
    [
        "--experiment-name",
        "--environment",
        "--generation-model",
        "--embedding-model",
        "--index-name",
    ],
)
def test_cli_requires_runtime_identity(
    missing_option,
):
    values = {
        "--experiment-name":
            "experiment",
        "--environment":
            "test",
        "--generation-model":
            "generation",
        "--embedding-model":
            "embedding",
        "--index-name":
            "index",
    }

    args = []

    for option, value in (
        values.items()
    ):
        if option == missing_option:
            continue

        args.extend(
            [
                option,
                value,
            ]
        )

    parser = build_parser()

    with pytest.raises(
        SystemExit,
    ):
        parser.parse_args(
            args
        )

def test_direct_script_entrypoint_help():
    repo_root = (
        Path(__file__)
        .resolve()
        .parents[1]
    )

    script = (
        repo_root
        / "scripts"
        / "run_llmops_evaluation.py"
    )

    completed = subprocess.run(
        [
            sys.executable,
            str(script),
            "--help",
        ],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == 0

    assert (
        "--experiment-name"
        in completed.stdout
    )

    assert (
        "--generation-model"
        in completed.stdout
    )

    assert (
        "--embedding-model"
        in completed.stdout
    )

    assert (
        "--index-name"
        in completed.stdout
    )
