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


# --- Automatic model download if missing ---
def ensure_model(model_name, model_path, hf_repo):
	import os
	if not os.path.exists(model_path):
		print(f"[INFO] Model '{model_name}' not found at {model_path}. Downloading from Hugging Face ({hf_repo})...")
		try:
			from transformers import AutoModelForSequenceClassification, AutoTokenizer, AutoModelForSequenceClassification, AutoModelForSeq2SeqLM
			import shutil
			os.makedirs(model_path, exist_ok=True)
			if model_name.lower() == "finbert":
				model = AutoModelForSequenceClassification.from_pretrained(hf_repo)
				tokenizer = AutoTokenizer.from_pretrained(hf_repo)
			elif model_name.lower() == "distilbart-mnli":
				model = AutoModelForSeq2SeqLM.from_pretrained(hf_repo)
				tokenizer = AutoTokenizer.from_pretrained(hf_repo)
			else:
				raise ValueError(f"Unknown model: {model_name}")
			model.save_pretrained(model_path)
			tokenizer.save_pretrained(model_path)
			print(f"[INFO] Downloaded and saved '{model_name}' to {model_path}")
		except Exception as e:
			print(f"[ERROR] Failed to download {model_name}: {e}")
	else:
		print(f"[INFO] Model '{model_name}' found at {model_path}")

# Download models if missing
ensure_model("finbert", FINBERT_PATH, "yiyanghkust/finbert-tone")
ensure_model("distilbart-mnli", DISTILBART_PATH, "facebook/bart-large-mnli")

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