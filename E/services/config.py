import os
from dotenv import load_dotenv

# Anchor everything to /app inside Docker, or the actual project root locally
APP_DIR = os.environ.get("APP_DIR", os.path.join(os.path.dirname(__file__), ".."))
APP_DIR = os.path.normpath(APP_DIR)

load_dotenv(os.path.join(APP_DIR, ".env"))

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")
TOPIC = os.getenv("KAFKA_TOPIC", "Financenews-raw")
MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY", "")

MODELS_DIR = os.getenv("MODELS_DIR", os.path.join(APP_DIR, "models"))

DISTILBART_PATH = os.path.join(MODELS_DIR, "distilbart-mnli")
FINBERT_PATH    = os.path.join(MODELS_DIR, "finbert")

USE_DISTILBART = os.path.exists(DISTILBART_PATH)
USE_FINBERT    = os.path.exists(FINBERT_PATH)

# Debug — remove after confirming
print(f"[DEBUG] MODELS_DIR: {MODELS_DIR}")
print(f"[DEBUG] USE_DISTILBART: {USE_DISTILBART} → {DISTILBART_PATH}")
print(f"[DEBUG] USE_FINBERT:    {USE_FINBERT} → {FINBERT_PATH}")

OLLAMA_URL   = os.getenv("OLLAMA_URL", "http://ollama:11434/api/generate")
OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "qwen2.5:3b")
# OLLAMA_MODEL = os.getenv("OLLAMA_MODEL", "deepseek-r1:1.5b")

LOGS_DIR     = os.path.join(APP_DIR, "logs")
ARCHIVE_PATH = os.path.join(APP_DIR, "news_archive.jsonl")