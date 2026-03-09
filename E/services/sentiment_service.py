import time
from .config import FINBERT_PATH, USE_FINBERT
from .logging_setup import logger, sentiment_logger

FINBERT_CLASSIFIER = None


def load_finbert_classifier():
    global FINBERT_CLASSIFIER

    if FINBERT_CLASSIFIER is not None:
        return FINBERT_CLASSIFIER

    if not USE_FINBERT:
        logger.warning("FinBERT model not available")
        return None

    try:
        logger.info("Loading FinBERT sentiment model...")
        from transformers import pipeline

        FINBERT_CLASSIFIER = pipeline(
            "sentiment-analysis",
            model=FINBERT_PATH,
            device=-1,
            max_length=512,
            truncation=True,
        )

        logger.info("FinBERT model loaded successfully")
        print("FinBERT sentiment model loaded")
        return FINBERT_CLASSIFIER
    except Exception as error:
        logger.error(f"Failed to load FinBERT model: {str(error)}")
        print(f"FinBERT loading error: {error}")
        return None


def analyze_sentiment_with_finbert(text):
    global FINBERT_CLASSIFIER

    if not text or len(text.strip()) < 10:
        return None, None

    try:
        if FINBERT_CLASSIFIER is None:
            FINBERT_CLASSIFIER = load_finbert_classifier()

        if FINBERT_CLASSIFIER is None:
            return None, None

        text_short = text[:512] if len(text) > 512 else text

        start_time = time.time()
        result = FINBERT_CLASSIFIER(text_short)[0]
        inference_time = time.time() - start_time

        label = result["label"].lower()
        score = result["score"]

        if label == "positive":
            sentiment_score = score
        elif label == "negative":
            sentiment_score = -score
        else:
            sentiment_score = 0.0

        logger.info(
            f"FinBERT sentiment: {label} ({sentiment_score:.3f}), confidence: {score:.3f}, time: {inference_time:.2f}s"
        )

        return sentiment_score, score
    except Exception as error:
        logger.error(f"FinBERT sentiment analysis failed: {str(error)}")
        return None, None


def compare_and_select_sentiment(
    marketaux_sentiment,
    marketaux_confidence,
    finbert_sentiment,
    finbert_confidence,
    article_num,
):
    if marketaux_sentiment is not None and finbert_sentiment is not None:
        same_direction = (marketaux_sentiment * finbert_sentiment) >= 0
        sentiment_diff = abs(marketaux_sentiment - finbert_sentiment)

        if same_direction and sentiment_diff < 0.3:
            total_conf = (marketaux_confidence or 0.5) + (finbert_confidence or 0.5)
            weighted_sentiment = (
                (marketaux_sentiment * (marketaux_confidence or 0.5))
                + (finbert_sentiment * (finbert_confidence or 0.5))
            ) / total_conf

            final_confidence = (marketaux_confidence or 0.5) + (finbert_confidence or 0.5)
            final_confidence = min(final_confidence, 1.0)

            sentiment_logger.info(
                f"Article {article_num}: AGREEMENT - Weighted average used - "
                f"MarketAux: {marketaux_sentiment:.3f} ({marketaux_confidence:.3f}), "
                f"FinBERT: {finbert_sentiment:.3f} ({finbert_confidence:.3f}), "
                f"Final: {weighted_sentiment:.3f} ({final_confidence:.3f})"
            )
            return weighted_sentiment, final_confidence, "HYBRID_AGREEMENT"

        if (finbert_confidence or 0) > (marketaux_confidence or 0):
            sentiment_logger.info(
                f"Article {article_num}: DISAGREEMENT - FinBERT selected (higher confidence)"
            )
            return finbert_sentiment, finbert_confidence, "FINBERT_DOMINANT"

        sentiment_logger.info(
            f"Article {article_num}: DISAGREEMENT - MarketAux selected (higher confidence)"
        )
        return marketaux_sentiment, marketaux_confidence, "MARKETAUX_DOMINANT"

    if marketaux_sentiment is not None:
        sentiment_logger.info(f"Article {article_num}: MARKETAUX_ONLY")
        return marketaux_sentiment, marketaux_confidence, "MARKETAUX_ONLY"

    if finbert_sentiment is not None:
        sentiment_logger.info(f"Article {article_num}: FINBERT_FALLBACK")
        return finbert_sentiment, finbert_confidence, "FINBERT_FALLBACK"

    sentiment_logger.warning(f"Article {article_num}: NO_SENTIMENT - Both sources failed")
    return None, None, "NO_SENTIMENT"
