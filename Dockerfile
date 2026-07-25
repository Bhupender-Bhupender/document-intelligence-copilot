# Document Intelligence Copilot
# Single-stage container image.
#
# Build:
#   docker build -t document-intelligence-copilot .
#
# Run (local defaults — all backends local, Ollama on host):
#   docker run -p 7860:7860 \
#     -e OLLAMA_BASE_URL=http://host.docker.internal:11434 \
#     document-intelligence-copilot
#
# Run (Azure backends — set env vars as needed):
#   docker run -p 7860:7860 \
#     --env-file .env \
#     document-intelligence-copilot
#
# The container exposes the Gradio UI on port 7860.
# Ollama is NOT bundled in this image; point OLLAMA_BASE_URL at any
# running Ollama instance (local daemon, sidecar, or remote endpoint).

FROM python:3.12-slim

# OpenMP — required by torch and several HuggingFace packages on slim base.
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgomp1 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies before copying source so Docker can cache
# this layer independently of application code changes.
COPY requirements-container.txt .
RUN pip install --no-cache-dir -r requirements-container.txt

# Copy the full project.
COPY . .

# Gradio UI port.
EXPOSE 7860

# run.py defines main() and guards execution with __main__.
CMD ["python", "run.py"]
