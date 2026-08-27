"""
Combined serving runtime.

FastAPI owns the HTTP application.
Gradio is mounted into the same ASGI process.

Routes:

    /api/v1/health
    /api/v1/ready
    /api/v1/retrieve
    /api/v1/answer

    /               Gradio UI

No internal HTTP hop exists between Gradio and FastAPI. Both use the same
Python serving layer.
"""

from __future__ import annotations

from fastapi import FastAPI


def create_runtime() -> FastAPI:
    """
    Build the combined ASGI application.

    Gradio import is intentionally deferred until runtime creation.
    """
    import gradio as gr

    from app.api import create_app
    from app.ui import build_ui

    api = create_app()
    demo = build_ui()

    mounted = gr.mount_gradio_app(
        api,
        demo,
        path="/",
    )

    return mounted


app = create_runtime()
