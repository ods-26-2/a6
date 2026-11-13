FROM python:3.12-slim

ENV DEBIAN_FRONTEND=noninteractive \
    OLLAMA_HOST=127.0.0.1:11434 \
    OLLAMA_MODELS=/models \
    PYTHONUNBUFFERED=1

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates curl zstd \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /models

RUN curl -fsSL https://ollama.com/install.sh | sh

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py entrypoint.sh ./
RUN chmod +x /app/entrypoint.sh

# Baixa o modelo durante o build para evitar download no cold start.
RUN set -eux; \
    ollama serve >/tmp/ollama-build.log 2>&1 & \
    OLLAMA_PID="$!"; \
    READY=0; \
    for i in $(seq 1 90); do \
      if curl -fsS http://127.0.0.1:11434/api/tags >/dev/null 2>&1; then READY=1; break; fi; \
      sleep 2; \
    done; \
    if [ "$READY" -ne 1 ]; then cat /tmp/ollama-build.log; exit 1; fi; \
    ollama pull qwen3:4b; \
    kill "$OLLAMA_PID"; \
    wait "$OLLAMA_PID" || true

EXPOSE 8080
ENTRYPOINT ["/app/entrypoint.sh"]
