from pathlib import Path

import pytest


mlflow = pytest.importorskip(
    'mlflow'
)

pytest.importorskip(
    'alembic'
)

pytest.importorskip(
    'sqlalchemy'
)


from mlflow.tracking import MlflowClient

from src.llmops.mlflow_tracking import (
    MLflowExperimentConfig,
    build_run_tags,
    log_metrics,
    log_params,
    start_llmops_run,
)

from src.llmops.versioning import (
    build_version_context,
)


def test_versioned_mlflow_run_round_trip(
    tmp_path: Path,
) -> None:
    previous_uri = (
        mlflow.get_tracking_uri()
    )

    db_path = (
        tmp_path
        / 'mlflow.db'
    )

    tracking_uri = (
        'sqlite:///'
        + db_path.as_posix()
    )

    config = MLflowExperimentConfig(
        experiment_name=(
            'phase15-pytest-integration'
        ),
        environment='test',
        tracking_uri=tracking_uri,
    )

    version_context = (
        build_version_context(
            generation_model=(
                'test-generation-model'
            ),
            embedding_model=(
                'test-embedding-model'
            ),
            index_name=(
                'test-search-index'
            ),
            evaluation_dataset_version=(
                'test-dataset-v1'
            ),
            code_revision=(
                'test-revision'
            ),
        )
    )

    extra_tags = {
        'validation_case':
            'pytest-integration',
    }

    expected_tags = (
        build_run_tags(
            version_context,
            environment='test',
            extra_tags=extra_tags,
        )
    )

    try:
        with start_llmops_run(
            config=config,
            version_context=(
                version_context
            ),
            run_name=(
                'versioned-round-trip'
            ),
            extra_tags=extra_tags,
        ) as run:
            run_id = (
                run.info.run_id
            )

            logged_params = (
                log_params(
                    {
                        'retrieval_top_k':
                            10,
                        'rerank_top_k':
                            5,
                        'evaluation_mode':
                            'integration',
                    }
                )
            )

            logged_metrics = (
                log_metrics(
                    {
                        'hit_at_1':
                            1.0,
                        'citation_valid_rate':
                            1.0,
                    }
                )
            )

        client = MlflowClient()

        saved = client.get_run(
            run_id
        )

        assert run_id

        for key, value in (
            expected_tags.items()
        ):
            assert (
                saved.data.tags.get(
                    key
                )
                == value
            )

        for key, value in (
            logged_params.items()
        ):
            assert (
                saved.data.params.get(
                    key
                )
                == value
            )

        for key, value in (
            logged_metrics.items()
        ):
            assert (
                saved.data.metrics.get(
                    key
                )
                == value
            )

        assert (
            saved.data.tags.get(
                'code_revision'
            )
            == 'test-revision'
        )

    finally:
        mlflow.end_run()

        mlflow.set_tracking_uri(
            previous_uri
        )
