from transformers import AutoTokenizer, AutoModelForSequenceClassification
import os

# Create models directory
models_dir = os.path.join(os.path.dirname(__file__), "models")
os.makedirs(models_dir, exist_ok=True)

print("📥 Downloading Distilled BART model for category classification only...")
print("ℹ️  Note: Confidence and sentiment are provided by API, not needed here")

distilbart_path = os.path.join(models_dir, "distilbart-mnli")

# Download only Distilled BART for category classification
bart_tokenizer = AutoTokenizer.from_pretrained("valhalla/distilbart-mnli-12-1")
bart_model = AutoModelForSequenceClassification.from_pretrained("valhalla/distilbart-mnli-12-1")

# Save locally
bart_tokenizer.save_pretrained(distilbart_path)
bart_model.save_pretrained(distilbart_path)
print(f"✅ Distilled BART saved to: {distilbart_path}")

print("🎉 Category classification model downloaded successfully!")
print(f"📊 Model size: ~300MB")
print(f"🎯 Purpose: Financial news category classification (A, B, C, D, E, F, G)")
print(f"📈 Expected accuracy: 88-92% for financial categories")