import time
import redis
from .config import DISTILBART_PATH, USE_DISTILBART
from .logging_setup import logger, fallback_logger

DISTILBART_CLASSIFIER = None

redis_client = redis.Redis(host="localhost", port=6379, decode_responses=True)

CATEGORY_LETTERS = "ABCDEFGHIJKLMN"
CATEGORY_LIST = list(CATEGORY_LETTERS)

keywords_dict = {}
for category in CATEGORY_LETTERS:
    redis_key = f"category_keywords_{category}"
    keywords_dict[category] = set(redis_client.smembers(redis_key))


CATEGORY_LABELS = [
    "earnings and quarterly results",
    "analyst ratings and recommendations",
    "mergers, acquisitions and deals",
    "regulatory and legal developments",
    "economic data and central banks",
    "corporate actions and leadership",
    "market trends and sector analysis",
    "ipo and new listings",
    "product launches and innovation",
    "general business and industry news",
    "commodities and energy",
    "esg and sustainability",
    "macro and geopolitics",
    "technology trends and disruption",
]

CATEGORY_MAP = {
    "earnings and quarterly results": "A",
    "analyst ratings and recommendations": "B",
    "mergers, acquisitions and deals": "C",
    "regulatory and legal developments": "D",
    "economic data and central banks": "E",
    "corporate actions and leadership": "F",
    "market trends and sector analysis": "G",
    "ipo and new listings": "H",
    "product launches and innovation": "I",
    "general business and industry news": "J",
    "commodities and energy": "K",
    "esg and sustainability": "L",
    "macro and geopolitics": "M",
    "technology trends and disruption": "N",
}


def load_distilbart_classifier():
    global DISTILBART_CLASSIFIER

    if DISTILBART_CLASSIFIER is not None:
        return DISTILBART_CLASSIFIER

    if not USE_DISTILBART:
        logger.warning("DistilBART model not available")
        return None

    try:
        logger.info("Loading DistilBART classifier...")
        from transformers import pipeline

        DISTILBART_CLASSIFIER = pipeline(
            "zero-shot-classification",
            model=DISTILBART_PATH,
            device=-1,
            max_length=256,
            truncation=True,
            padding=True,
        )

        logger.info("DistilBART classifier loaded successfully")
        print("DistilBART classifier loaded")
        return DISTILBART_CLASSIFIER
    except Exception as error:
        logger.error(f"Failed to load DistilBART classifier: {str(error)}")
        print(f"DistilBART loading error: {error}")
        return None


def classify_with_distilbart(text):
    global DISTILBART_CLASSIFIER

    if not USE_DISTILBART or not text or len(text.strip()) < 15:
        logger.info(
            f"DistilBART skipped: available={USE_DISTILBART}, text_length={len(text) if text else 0}"
        )
        return None

    try:
        logger.info("Starting DistilBART classification...")
        if DISTILBART_CLASSIFIER is None:
            DISTILBART_CLASSIFIER = load_distilbart_classifier()

        if DISTILBART_CLASSIFIER is None:
            return None

        text_short = text[:200] if len(text) > 200 else text
        classification_start = time.time()
        result = DISTILBART_CLASSIFIER(text_short, CATEGORY_LABELS)
        classification_time = time.time() - classification_start

        predicted_category = result["labels"][0]
        confidence = result["scores"][0]
        final_category = CATEGORY_MAP.get(predicted_category, "J")

        logger.info(
            f"DistilBART classification SUCCESS - Category: {final_category} - "
            f"Label: '{predicted_category}' - Confidence: {confidence:.3f} - "
            f"Time: {classification_time:.2f}s"
        )

        return final_category
    except Exception as error:
        logger.error(f"DistilBART classification FAILED: {str(error)}")
        return None


def keyword_based_category(text):
    text_lower = text.lower()
    for category, keywords in keywords_dict.items():
        if any(word in text_lower for word in keywords):
            return category
    return "J"


def calculate_keyword_scores(text):
    text_lower = text.lower()
    category_scores = {}

    for category in CATEGORY_LIST:
        try:
            keywords = keywords_dict.get(category, set())
            if keywords:
                keyword_matches = 0
                total_keywords = len(keywords)
                match_weight = 0.0

                for keyword in keywords:
                    keyword_lower = keyword.lower()
                    if keyword_lower in text_lower:
                        occurrences = text_lower.count(keyword_lower)
                        specificity_weight = min(len(keyword.split()) * 0.2, 1.0)
                        frequency_weight = min(occurrences * 0.3, 1.0)
                        match_weight += (1.0 + specificity_weight + frequency_weight)
                        keyword_matches += 1

                if total_keywords > 0:
                    base_score = keyword_matches / total_keywords
                    strength_boost = min(match_weight / total_keywords, 0.5)
                    final_score = min(base_score + strength_boost, 1.0)
                    category_scores[category] = final_score
        except Exception as error:
            logger.error(f"Redis keyword scoring error for {category}: {error}")
            continue

    return category_scores


def check_rule_based_overrides(text):
    text_lower = text.lower()

    if any(term in text_lower for term in ["insider trading", "insider trade", "insider sold", "insider bought"]):
        return "F"
    if any(term in text_lower for term in ["ipo filing", "ipo launch", "going public", "initial public offering"]):
        return "H"
    if any(term in text_lower for term in ["merger agreement", "acquisition deal", "takeover bid"]):
        return "C"
    if any(term in text_lower for term in ["sec filing", "sec investigation", "regulatory probe"]):
        return "D"
    if any(term in text_lower for term in ["fed meeting", "interest rate decision", "monetary policy"]):
        return "E"

    return None


def weighted_category_classification(text, article_num):
    alpha = 0.7
    beta = 0.3
    min_threshold = 0.4

    category_scores = {}

    ml_scores = {}
    distilbart_category = classify_with_distilbart(text)
    distilbart_confidence = 0.8

    if distilbart_category:
        ml_scores[distilbart_category] = distilbart_confidence

    keyword_scores = calculate_keyword_scores(text)

    all_categories = set(
        list(ml_scores.keys())
        + list(keyword_scores.keys())
        + ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N"]
    )

    for category in all_categories:
        ml_score = ml_scores.get(category, 0.0)
        keyword_score = keyword_scores.get(category, 0.0)
        combined_score = (alpha * ml_score) + (beta * keyword_score)
        category_scores[category] = combined_score

    if category_scores:
        best_category = max(category_scores, key=category_scores.get)
        best_score = category_scores[best_category]

        score_breakdown = {
            "ml_score": ml_scores.get(best_category, 0.0),
            "keyword_score": keyword_scores.get(best_category, 0.0),
            "combined_score": best_score,
            "weights": {"alpha": alpha, "beta": beta},
            "top_3_combined": sorted(category_scores.items(), key=lambda item: item[1], reverse=True)[:3],
        }

        if best_score >= min_threshold:
            if ml_scores.get(best_category, 0.0) > 0.6:
                method = "WEIGHTED_ML_DOMINANT"
            elif keyword_scores.get(best_category, 0.0) > 0.7:
                method = "WEIGHTED_KEYWORD_DOMINANT"
            else:
                method = "WEIGHTED_BALANCED"

            logger.info(f"Article {article_num}: {method} - Category: {best_category}, Score: {best_score:.3f}")
            return best_category, best_score, method, score_breakdown

        rule_category = check_rule_based_overrides(text)
        if rule_category:
            logger.info(
                f"Article {article_num}: RULE OVERRIDE - Category: {rule_category}, Low weighted score: {best_score:.3f}"
            )
            return rule_category, 0.8, "RULE_OVERRIDE", score_breakdown

        logger.warning(f"Article {article_num}: DEFAULT CATEGORY - J, Low confidence: {best_score:.3f}")
        return "J", 0.3, "DEFAULT_LOW_CONFIDENCE", score_breakdown

    logger.warning(f"Article {article_num}: NO CLASSIFICATION SCORES - Default: J")
    return "J", 0.2, "DEFAULT_NO_SCORES", {}


def enhanced_classify_news_category(text, payload, article_num):
    try:
        category, confidence, method, breakdown = weighted_category_classification(text, article_num)

        logger.info(
            f"Article {article_num}: Classification Breakdown - ML: {breakdown.get('ml_score', 0.0):.3f}, "
            f"Keywords: {breakdown.get('keyword_score', 0.0):.3f}, Combined: {breakdown.get('combined_score', 0.0):.3f}"
        )

        fallback_logger.info(
            f"CLASSIFICATION,{method},{category},{confidence:.3f},{breakdown.get('ml_score', 0.0):.3f},{breakdown.get('keyword_score', 0.0):.3f}"
        )

        payload["category"] = category
        return category
    except Exception as error:
        logger.error(f"Article {article_num}: Enhanced classification error: {error}")
        try:
            category = keyword_based_category(text)
            payload["category"] = category
            logger.warning(f"Article {article_num}: FALLBACK to keyword classification: {category}")
            return category
        except Exception as fallback_error:
            logger.error(f"Article {article_num}: All classification methods failed: {fallback_error}")
            payload["category"] = "J"
            return "J"
