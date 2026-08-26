from __future__ import annotations

from src.core.config import config
from src.generation.databricks_llm import (
    generate_with_metadata,
)


def main() -> None:
    model = config.databricks_generation_model.strip()

    if not model:
        raise RuntimeError(
            "DATABRICKS_GENERATION_MODEL is not configured."
        )

    result = generate_with_metadata(
        [
            {
                "role": "system",
                "content": (
                    "Answer briefly and only from the "
                    "information supplied by the user."
                ),
            },
            {
                "role": "user",
                "content": (
                    "Evidence: Project Alpha uses Delta tables. "
                    "Question: What storage format does "
                    "Project Alpha use?"
                ),
            },
        ],
        model=model,
        max_tokens=256,
    )

    print(
        "PHASE12D_BACKEND:",
        "databricks",
    )
    print(
        "PHASE12D_MODEL_CONFIGURED:",
        bool(result.model),
    )
    print(
        "PHASE12D_ANSWER_NONEMPTY:",
        bool(result.text.strip()),
    )
    print(
        "PHASE12D_PROMPT_TOKENS_AVAILABLE:",
        result.prompt_tokens is not None,
    )
    print(
        "PHASE12D_COMPLETION_TOKENS_AVAILABLE:",
        result.completion_tokens is not None,
    )
    print(
        "PHASE12D_TOTAL_TOKENS_AVAILABLE:",
        result.total_tokens is not None,
    )
    print(
        "PHASE12D_FINISH_REASON_AVAILABLE:",
        result.finish_reason is not None,
    )

    passed = (
        bool(result.text.strip())
        and result.model == model
    )

    print(
        "PHASE12D_MANAGED_PASS:",
        passed,
    )

    if not passed:
        raise RuntimeError(
            "Phase 12D managed generation smoke failed."
        )


if __name__ == "__main__":
    main()
