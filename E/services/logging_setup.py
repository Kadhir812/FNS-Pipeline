import logging
import os
import sys
from datetime import datetime
from .config import LOGS_DIR

os.makedirs(LOGS_DIR, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(name)s] - %(message)s",
    handlers=[
        logging.FileHandler(
            os.path.join(LOGS_DIR, f"extract_paths_{datetime.now().strftime('%Y%m%d')}.log"),
            encoding="utf-8",
        ),
        logging.StreamHandler(sys.stdout),
    ],
)

logger = logging.getLogger("extract")

fallback_logger = logging.getLogger("fallback_tracker")
fallback_handler = logging.FileHandler(
    os.path.join(LOGS_DIR, f"fallback_usage_{datetime.now().strftime('%Y%m%d')}.log"),
    encoding="utf-8",
)
fallback_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
fallback_logger.addHandler(fallback_handler)
fallback_logger.setLevel(logging.INFO)

sentiment_logger = logging.getLogger("sentiment_tracker")
sentiment_handler = logging.FileHandler(
    os.path.join(LOGS_DIR, f"sentiment_analysis_{datetime.now().strftime('%Y%m%d')}.log"),
    encoding="utf-8",
)
sentiment_handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
sentiment_logger.addHandler(sentiment_handler)
sentiment_logger.setLevel(logging.INFO)
