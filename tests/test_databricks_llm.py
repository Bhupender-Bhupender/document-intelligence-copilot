from types import SimpleNamespace

import pytest

from src.generation.databricks_llm import (
    DatabricksGenerationError,
    generate,
    generate_with_metadata,
)


class FakeCompletions:
    def __init__(self, response):
        self.response = response
        self.kwargs = None

    def create(self, **kwargs):
        self.kwargs = kwargs
        return self.response


class FakeClient:
    def __init__(self, response):
        self.chat = SimpleNamespace(
            completions=FakeCompletions(response)
        )


def _response(text="Grounded answer."):
    return SimpleNamespace(
        choices=[
            SimpleNamespace(
                message=SimpleNamespace(
                    content=text,
                ),
                finish_reason="stop",
            )
        ],
        usage=SimpleNamespace(
            prompt_tokens=100,
            completion_tokens=20,
            total_tokens=120,
        ),
    )


def test_generate_with_metadata_maps_response():
    client = FakeClient(_response())

    result = generate_with_metadata(
        [{"role": "user", "content": "Question"}],
        model="system.ai.test-model",
        _client=client,
    )

    assert result.text == "Grounded answer."
    assert result.model == "system.ai.test-model"
    assert result.prompt_tokens == 100
    assert result.completion_tokens == 20
    assert result.total_tokens == 120
    assert result.finish_reason == "stop"


def test_generate_uses_chat_completion_contract():
    client = FakeClient(_response())

    generate_with_metadata(
        [{"role": "user", "content": "Question"}],
        model="system.ai.test-model",
        max_tokens=64,
        _client=client,
    )

    kwargs = client.chat.completions.kwargs

    assert kwargs["model"] == "system.ai.test-model"
    assert kwargs["max_tokens"] == 64
    assert kwargs["messages"] == [
        {
            "role": "user",
            "content": "Question",
        }
    ]


def test_empty_messages_rejected():
    with pytest.raises(
        ValueError,
        match="messages must not be empty",
    ):
        generate_with_metadata(
            [],
            model="system.ai.test-model",
            _client=FakeClient(_response()),
        )


def test_blank_model_rejected(monkeypatch):
    from src.core.config import config

    monkeypatch.setattr(
        config,
        "databricks_generation_model",
        "",
    )

    with pytest.raises(
        DatabricksGenerationError,
        match="model is not configured",
    ):
        generate_with_metadata(
            [{"role": "user", "content": "Question"}],
            model=None,
            _client=FakeClient(_response()),
        )


def test_empty_provider_answer_rejected():
    client = FakeClient(_response("   "))

    with pytest.raises(
        DatabricksGenerationError,
        match="empty answer",
    ):
        generate_with_metadata(
            [{"role": "user", "content": "Question"}],
            model="system.ai.test-model",
            _client=client,
        )


def test_text_only_wrapper():
    client = FakeClient(_response("Answer"))

    result = generate_with_metadata(
        [{"role": "user", "content": "Question"}],
        model="system.ai.test-model",
        _client=client,
    )

    assert result.text == "Answer"


def test_structured_text_content_is_supported():
    response = _response()

    response.choices[0].message.content = [
        {
            "type": "reasoning",
            "summary": [
                {
                    "type": "summary_text",
                    "text": "Internal reasoning",
                }
            ],
        },
        {
            "type": "text",
            "text": "Final grounded answer.",
        },
    ]

    client = FakeClient(response)

    result = generate_with_metadata(
        [{"role": "user", "content": "Question"}],
        model="system.ai.test-model",
        _client=client,
    )

    assert result.text == "Final grounded answer."


def test_reasoning_only_content_is_rejected():
    response = _response()

    response.choices[0].message.content = [
        {
            "type": "reasoning",
            "summary": [],
        }
    ]

    client = FakeClient(response)

    with pytest.raises(
        DatabricksGenerationError,
        match="no visible answer text",
    ):
        generate_with_metadata(
            [{"role": "user", "content": "Question"}],
            model="system.ai.test-model",
            _client=client,
        )



def test_resolve_token_prefers_environment(
    monkeypatch,
):
    from src.generation.databricks_llm import (
        _resolve_token,
    )

    monkeypatch.setenv(
        "DATABRICKS_TOKEN",
        "local-dev-token",
    )

    class ExplodingClient:
        @property
        def config(self):
            raise AssertionError(
                "SDK fallback must not run"
            )

    assert _resolve_token(
        _workspace_client=ExplodingClient()
    ) == "local-dev-token"


def test_resolve_token_supports_unified_app_auth(
    monkeypatch,
):
    from src.generation.databricks_llm import (
        _resolve_token,
    )

    monkeypatch.delenv(
        "DATABRICKS_TOKEN",
        raising=False,
    )

    class FakeConfig:
        def authenticate(self):
            return {
                "Authorization":
                    "Bearer app-oauth-token"
            }

    class FakeWorkspaceClient:
        config = FakeConfig()

    assert _resolve_token(
        _workspace_client=(
            FakeWorkspaceClient()
        )
    ) == "app-oauth-token"


def test_resolve_token_rejects_missing_bearer(
    monkeypatch,
):
    import pytest

    from src.generation.databricks_llm import (
        DatabricksGenerationError,
        _resolve_token,
    )

    monkeypatch.delenv(
        "DATABRICKS_TOKEN",
        raising=False,
    )

    class FakeConfig:
        def authenticate(self):
            return {}

    class FakeWorkspaceClient:
        config = FakeConfig()

    with pytest.raises(
        DatabricksGenerationError,
        match="bearer token",
    ):
        _resolve_token(
            _workspace_client=(
                FakeWorkspaceClient()
            )
        )



def test_safe_generation_error_metadata_reads_provider_status():
    from src.generation.databricks_llm import (
        _safe_generation_error_metadata,
    )

    class FakeProviderError(Exception):
        status_code = 403
        code = "PERMISSION_DENIED"

    metadata = (
        _safe_generation_error_metadata(
            FakeProviderError(
                "private provider message"
            )
        )
    )

    assert metadata == {
        "cause_type":
            "FakeProviderError",
        "status_code":
            403,
        "error_code":
            "PERMISSION_DENIED",
    }

    assert (
        "private provider message"
        not in str(metadata)
    )


def test_safe_generation_error_metadata_reads_chained_cause():
    from src.generation.databricks_llm import (
        DatabricksGenerationError,
        _safe_generation_error_metadata,
    )

    class FakeAuthError(Exception):
        status_code = 401
        error_code = "AUTHENTICATION_FAILED"

    try:
        try:
            raise FakeAuthError(
                "private auth detail"
            )
        except FakeAuthError as exc:
            raise DatabricksGenerationError(
                "wrapped"
            ) from exc

    except DatabricksGenerationError as exc:
        metadata = (
            _safe_generation_error_metadata(
                exc
            )
        )

    assert metadata[
        "cause_type"
    ] == "FakeAuthError"

    assert metadata[
        "status_code"
    ] == 401

    assert metadata[
        "error_code"
    ] == "AUTHENTICATION_FAILED"

    assert (
        "private auth detail"
        not in str(metadata)
    )
