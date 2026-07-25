"""
Container and local-app entrypoint for the Document Intelligence Copilot.

Usage
-----
    python run.py

The Gradio server binds to the address and port specified by environment
variables (see .env.example for the full runtime contract).

Runtime env vars (with defaults):
    GRADIO_SERVER_NAME   bind address  (default: "0.0.0.0" — required in
                         container context; use "127.0.0.1" for local-only)
    GRADIO_SERVER_PORT   TCP port      (default: 7860)

Import safety
-------------
build_ui() and demo.launch() live inside main() so that importing this
module never starts the server.  Tests can safely do:

    from run import main
"""
from __future__ import annotations

import os


def main() -> None:
    """Build the Gradio app and start serving."""
    from app.ui import build_ui  # deferred — keeps module-level import light

    demo = build_ui()
    demo.launch(
        server_name=os.environ.get("GRADIO_SERVER_NAME", "0.0.0.0"),
        server_port=int(os.environ.get("GRADIO_SERVER_PORT", "7860")),
        share=False,
    )


if __name__ == "__main__":
    main()
