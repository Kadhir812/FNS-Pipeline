#!/bin/sh
# Entrypoint for Ollama: ensure qwen2.5:3b model is present, pull if missing, then start Ollama

MODEL_NAME="qwen2.5:3b"
OLLAMA_HOST="http://localhost:11434"

# Wait for Ollama server to be up (in case it's needed for model check)
wait_for_ollama() {
  for i in $(seq 1 20); do
    if curl -sf "$OLLAMA_HOST/api/tags" >/dev/null; then
      return 0
    fi
    sleep 1
  done
  return 1
}

# Start Ollama in the background for model check
ollama serve &
OLLAMA_PID=$!

# Wait for Ollama API to be ready
wait_for_ollama

# Check if model exists
if ! curl -sf "$OLLAMA_HOST/api/tags" | grep -q "$MODEL_NAME"; then
  echo "Model $MODEL_NAME not found, pulling..."
  ollama pull "$MODEL_NAME"
else
  echo "Model $MODEL_NAME already exists."
fi

# Kill background Ollama (if still running)
kill $OLLAMA_PID 2>/dev/null || true

# Start Ollama in foreground
exec ollama serve
