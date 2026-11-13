#!/bin/sh
set -eu

export OLLAMA_HOST="127.0.0.1:11434"
export OLLAMA_MODELS="/models"

# Ollama inicia em segundo plano. Nao bloqueamos a abertura da porta HTTP
# esperando pela API do modelo: o Cloud Run precisa encontrar PORT escutando.
ollama serve >/tmp/ollama.log 2>&1 &
OLLAMA_PID=$!

# FastAPI sobe imediatamente em 0.0.0.0:$PORT.
uvicorn app:app --host 0.0.0.0 --port "${PORT:-8080}" --workers 1 &
API_PID=$!

shutdown() {
  kill "$API_PID" "$OLLAMA_PID" 2>/dev/null || true
  wait "$API_PID" 2>/dev/null || true
  wait "$OLLAMA_PID" 2>/dev/null || true
}
trap shutdown EXIT INT TERM
wait "$API_PID"
