import os
from dotenv import load_dotenv

SERVICE_DIR = os.path.dirname(__file__)
BASE_DIR = os.path.dirname(SERVICE_DIR)
ROOT_DIR = os.path.dirname(BASE_DIR)

# Load environment variables from E/.env
load_dotenv(os.path.join(BASE_DIR, ".env"))

KAFKA_BROKER = "localhost:29092"
TOPIC = "Financenews-raw"
MARKETAUX_API_KEY = os.getenv("MARKETAUX_API_KEY", "")

DISTILBART_PATH = os.path.join(ROOT_DIR, "T", "models", "distilbart-mnli")
FINBERT_PATH = os.path.join(ROOT_DIR, "T", "models", "finbert")

USE_DISTILBART = os.path.exists(DISTILBART_PATH)
USE_FINBERT = os.path.exists(FINBERT_PATH)

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "deepseek-r1:1.5b"

LOGS_DIR = os.path.join(BASE_DIR, "logs")
ARCHIVE_PATH = os.path.join(BASE_DIR, "news_archive.jsonl")
