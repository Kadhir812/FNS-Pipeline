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
        return FINBERT_CLASSIFIER
    except Exception as error:
        logger.error(f"Failed to load FinBERT model: {str(error)}")
        return None


def analyze_sentiment_with_finbert(text, title="", description=""):
    global FINBERT_CLASSIFIER

    if not text or len(text.strip()) < 10:
        return None, None

    try:
        if FINBERT_CLASSIFIER is None:
            FINBERT_CLASSIFIER = load_finbert_classifier()

        if FINBERT_CLASSIFIER is None:
            return None, None

        # ✅ title + description + content — same fix as DistilBART
        parts = []
        if title and len(title.strip()) > 5:
            parts.append(title.strip())
        if description and len(description.strip()) > 10:
            parts.append(description.strip())
        if text and len(text.strip()) > 10:
            parts.append(text.strip())

        combined   = " ".join(parts)
        text_input = combined[:512] if len(combined) > 512 else combined

        start_time     = time.time()
        result         = FINBERT_CLASSIFIER(text_input)[0]
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
            f"FinBERT sentiment: {label} ({sentiment_score:.3f}), "
            f"confidence: {score:.3f}, time: {inference_time:.2f}s, "
            f"input_chars: {len(text_input)}"
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

        if same_direction and sentiment_diff < 0.5:  # ✅ loosened from 0.3
            ma_conf  = marketaux_confidence or 0.5
            fb_conf  = finbert_confidence   or 0.5
            total    = ma_conf + fb_conf

            weighted_sentiment = (
                (marketaux_sentiment * ma_conf) +
                (finbert_sentiment   * fb_conf)
            ) / total

            # ✅ Average — stays in 0–1 range
            final_confidence = (ma_conf + fb_conf) / 2

            sentiment_logger.info(
                f"Article {article_num}: AGREEMENT - "
                f"MarketAux: {marketaux_sentiment:.3f} ({ma_conf:.3f}), "
                f"FinBERT: {finbert_sentiment:.3f} ({fb_conf:.3f}), "
                f"Weighted final: {weighted_sentiment:.3f} ({final_confidence:.3f}), "
                f"diff: {sentiment_diff:.3f}"
            )
            return weighted_sentiment, final_confidence, "HYBRID_AGREEMENT"

        # Disagreement — pick higher confidence
        ma_conf = marketaux_confidence or 0
        fb_conf = finbert_confidence   or 0

        if fb_conf > ma_conf:
            sentiment_logger.info(
                f"Article {article_num}: DISAGREEMENT - FinBERT selected "
                f"(FinBERT: {finbert_sentiment:.3f} @ {fb_conf:.3f} > "
                f"MarketAux: {marketaux_sentiment:.3f} @ {ma_conf:.3f}), "
                f"diff: {sentiment_diff:.3f}"
            )
            return finbert_sentiment, finbert_confidence, "FINBERT_DOMINANT"

        sentiment_logger.info(
            f"Article {article_num}: DISAGREEMENT - MarketAux selected "
            f"(MarketAux: {marketaux_sentiment:.3f} @ {ma_conf:.3f} > "
            f"FinBERT: {finbert_sentiment:.3f} @ {fb_conf:.3f}), "
            f"diff: {sentiment_diff:.3f}"
        )
        return marketaux_sentiment, marketaux_confidence, "MARKETAUX_DOMINANT"

    if marketaux_sentiment is not None:
        sentiment_logger.info(
            f"Article {article_num}: MARKETAUX_ONLY "
            f"({marketaux_sentiment:.3f} @ {marketaux_confidence:.3f})"
        )
        return marketaux_sentiment, marketaux_confidence, "MARKETAUX_ONLY"

    if finbert_sentiment is not None:
        sentiment_logger.info(
            f"Article {article_num}: FINBERT_FALLBACK "
            f"({finbert_sentiment:.3f} @ {finbert_confidence:.3f})"
        )
        return finbert_sentiment, finbert_confidence, "FINBERT_FALLBACK"

    sentiment_logger.warning(
        f"Article {article_num}: NO_SENTIMENT - both sources failed"
    )
    return None, None, "NO_SENTIMENT"