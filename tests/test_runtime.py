from __future__ import annotations

from fastapi.testclient import TestClient

from app.runtime import app


def test_combined_runtime_keeps_api_routes():
    client = TestClient(app)

    response = client.get(
        "/api/v1/health"
    )

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_combined_runtime_serves_gradio_at_root():
    """
    Validate the public serving contract rather than inspecting
    Starlette's internal Mount representation.
    """
    client = TestClient(app)

    response = client.get(
        "/",
        follow_redirects=True,
    )

    assert response.status_code == 200

    content_type = response.headers.get(
        "content-type",
        "",
    )

    assert "text/html" in content_type.lower()
