import os
import time
import logging
from transformers import AutoModelForSequenceClassification, AutoTokenizer

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("finbert_download.log"),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger("FinBERTDownloader")

def download_finbert(save_path="./models/finbert"):
    """
    Download ProsusAI/finbert model and tokenizer and save locally
    
    Args:
        save_path: Directory to save the model
    
    Returns:
        Path to saved model
    """
    try:
        start_time = time.time()
        logger.info(f"Starting download of ProsusAI/finbert model")
        
        # Create directory if it doesn't exist
        os.makedirs(save_path, exist_ok=True)
        
        # Download model and tokenizer
        logger.info("Downloading tokenizer...")
        tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        
        logger.info("Downloading model...")
        model = AutoModelForSequenceClassification.from_pretrained("ProsusAI/finbert")
        
        # Save tokenizer and model locally
        logger.info(f"Saving tokenizer to {save_path}")
        tokenizer.save_pretrained(save_path)
        
        logger.info(f"Saving model to {save_path}")
        model.save_pretrained(save_path)
        
        # Check model size
        model_size_bytes = sum(p.numel() * p.element_size() for p in model.parameters())
        model_size_mb = model_size_bytes / (1024 * 1024)
        
        end_time = time.time()
        elapsed = end_time - start_time
        
        logger.info(f"Download complete in {elapsed:.2f} seconds")
        logger.info(f"Model size: {model_size_mb:.2f} MB")
        logger.info(f"Model saved to: {os.path.abspath(save_path)}")
        
        # Verify files exist
        files = os.listdir(save_path)
        logger.info(f"Files saved: {', '.join(files[:5])}{'...' if len(files) > 5 else ''}")
        
        return os.path.abspath(save_path)
        
    except Exception as e:
        logger.error(f"Error downloading model: {str(e)}")
        raise

if __name__ == "__main__":
    try:
        # Directory to save model within T folder
        script_dir = os.path.dirname(os.path.abspath(__file__))
        models_dir = os.path.join(script_dir, "models")
        model_dir = os.path.join(models_dir, "finbert")
        
        logger.info(f"Model will be saved to: {model_dir}")
        
        # Download model
        model_path = download_finbert(model_dir)
        
        print("\n" + "=" * 60)
        print(f"✅ FinBERT model successfully downloaded!")
        print(f"📂 Model location: {model_path}")
        print(f"🚀 Ready for use in your financial sentiment analysis pipeline")
        print("=" * 60)
        
    except Exception as e:
        print(f"\n❌ Error in download process: {str(e)}")