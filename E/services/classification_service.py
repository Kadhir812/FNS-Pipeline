import re
import time
import redis
from .config import DISTILBART_PATH, USE_DISTILBART
from .logging_setup import logger, fallback_logger

DISTILBART_CLASSIFIER = None

# ============================================================
# REDIS — keyword store
# ============================================================
redis_client = redis.Redis(host="redis", port=6379, decode_responses=True)

CATEGORY_LETTERS = "ABCDEFGHIJKLMN"
CATEGORY_LIST    = list(CATEGORY_LETTERS)

keywords_dict = {}
for category in CATEGORY_LETTERS:
    redis_key = f"category_keywords_{category}"
    keywords_dict[category] = set(redis_client.smembers(redis_key))

# ============================================================
# COMPILED REGEX PATTERNS
# ✅ Built once at module load — O(M) per category vs O(N×M)
# ============================================================
keyword_patterns = {}
for category in CATEGORY_LETTERS:
    kws = keywords_dict.get(category, set())
    if kws:
        escaped = [re.escape(w.lower()) for w in kws]
        pattern = "|".join(escaped)
        keyword_patterns[category] = re.compile(pattern, re.IGNORECASE)
    else:
        keyword_patterns[category] = None

# ============================================================
# LABELS & MAP
# ============================================================
CATEGORY_LABELS = [
    "earnings, quarterly results, fund commentary and financial performance",  # A
    "analyst ratings, price targets and stock recommendations",                # B
    "mergers, acquisitions, deals and takeovers",                             # C
    "regulatory, legal, compliance and government investigations",             # D
    "economic data, central banks, interest rates and monetary policy",       # E
    "corporate actions, executive changes and leadership",                    # F
    "market trends, sector analysis and investment flows",                    # G
    "ipo, new listings, public offerings and stock debuts",                   # H
    "product launches, clinical trials, drug approvals and innovation",       # I
    "general business, company news and industry updates",                    # J
    "commodities, energy, oil, gas and raw materials",                        # K
    "esg, sustainability, climate and corporate responsibility",              # L
    "macro, geopolitics, trade wars, sanctions and political events",         # M
    "technology trends, ai, semiconductors and digital disruption",           # N
]

CATEGORY_MAP = {
    "earnings, quarterly results, fund commentary and financial performance":  "A",
    "analyst ratings, price targets and stock recommendations":                "B",
    "mergers, acquisitions, deals and takeovers":                             "C",
    "regulatory, legal, compliance and government investigations":             "D",
    "economic data, central banks, interest rates and monetary policy":       "E",
    "corporate actions, executive changes and leadership":                    "F",
    "market trends, sector analysis and investment flows":                    "G",
    "ipo, new listings, public offerings and stock debuts":                   "H",
    "product launches, clinical trials, drug approvals and innovation":       "I",
    "general business, company news and industry updates":                    "J",
    "commodities, energy, oil, gas and raw materials":                        "K",
    "esg, sustainability, climate and corporate responsibility":              "L",
    "macro, geopolitics, trade wars, sanctions and political events":         "M",
    "technology trends, ai, semiconductors and digital disruption":           "N",
}

# ============================================================
# RULE OVERRIDE THRESHOLD
# If Redis keyword score >= this, skip ML entirely
# ✅ Single source of truth — no hardcoded rule lists
# ============================================================
RULE_CONFIDENCE_THRESHOLD = 0.6

# ============================================================
# DISTILBART — load
# ============================================================
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
            max_length=512,
            truncation=True,
            padding=True,
        )
        logger.info("DistilBART classifier loaded successfully")
        return DISTILBART_CLASSIFIER

    except Exception as error:
        logger.error(f"Failed to load DistilBART classifier: {str(error)}")
        return None

# ============================================================
# DISTILBART — classify
# ✅ title + description + content for richer context
# ✅ Top-3 labels used — captures secondary signals
# ✅ Returns (best_category, best_confidence, ml_scores_dict)
# ============================================================
def classify_with_distilbart(text, title="", description=""):
    global DISTILBART_CLASSIFIER

    if not USE_DISTILBART or not text or len(text.strip()) < 15:
        logger.info(
            f"DistilBART skipped: available={USE_DISTILBART}, "
            f"text_length={len(text) if text else 0}"
        )
        return None, None, {}

    try:
        logger.info("Starting DistilBART classification...")
        if DISTILBART_CLASSIFIER is None:
            DISTILBART_CLASSIFIER = load_distilbart_classifier()

        if DISTILBART_CLASSIFIER is None:
            return None, None, {}

        # ✅ title first — highest signal, then description, then content
        parts = []
        if title and len(title.strip()) > 5:
            parts.append(title.strip())
        if description and len(description.strip()) > 10:
            parts.append(description.strip())
        if text and len(text.strip()) > 10:
            parts.append(text.strip())

        combined   = " ".join(parts)
        text_input = combined[:512] if len(combined) > 512 else combined

        logger.info(
            f"DistilBART input: {len(text_input)} chars "
            f"(title={len(title)}, desc={len(description)}, content={len(text)})"
        )

        classification_start = time.time()
        result               = DISTILBART_CLASSIFIER(text_input, CATEGORY_LABELS)
        classification_time  = time.time() - classification_start

        # ✅ Top-3 labels — captures secondary category signals
        top_n      = 3
        top_labels = result["labels"][:top_n]
        top_scores = result["scores"][:top_n]

        # Accumulate scores per category
        # (multiple labels can map to same category letter)
        ml_scores = {}
        for label, score in zip(top_labels, top_scores):
            category = CATEGORY_MAP.get(label, "J")
            ml_scores[category] = ml_scores.get(category, 0.0) + score

        # ✅ Normalize — prevents score inflation
        total = sum(ml_scores.values())
        if total > 0:
            ml_scores = {k: v / total for k, v in ml_scores.items()}

        best_category   = CATEGORY_MAP.get(top_labels[0], "J")
        best_confidence = top_scores[0]

        logger.info(
            f"DistilBART SUCCESS - Top-3: "
            + ", ".join(
                f"{CATEGORY_MAP.get(l, 'J')}={s:.3f}"
                for l, s in zip(top_labels, top_scores)
            )
            + f" — Time: {classification_time:.2f}s"
        )

        return best_category, best_confidence, ml_scores

    except Exception as error:
        logger.error(f"DistilBART classification FAILED: {str(error)}")
        return None, None, {}

# ============================================================
# KEYWORD — simple fallback
# ✅ Uses compiled regex patterns
# ============================================================
def keyword_based_category(text):
    text_lower = text.lower()
    for category in CATEGORY_LIST:
        pattern = keyword_patterns.get(category)
        if pattern and pattern.search(text_lower):
            return category
    return "J"

# ============================================================
# KEYWORD — weighted scoring
# ✅ Compiled regex — O(M) per category
# ✅ Unique matches + total occurrences tracked separately
# ============================================================
def calculate_keyword_scores(text):
    text_lower      = text.lower()
    category_scores = {}

    for category in CATEGORY_LIST:
        pattern = keyword_patterns.get(category)
        if not pattern:
            continue

        try:
            matches = pattern.findall(text_lower)
            if not matches:
                continue

            unique_matches    = set(matches)
            total_keywords    = len(keywords_dict.get(category, set()))
            keyword_matches   = len(unique_matches)
            total_occurrences = len(matches)

            if total_keywords > 0 and keyword_matches > 0:
                base_score = keyword_matches / total_keywords

                # Multi-word keywords score higher — more specific signal
                specificity_boost = sum(
                    min(len(m.split()) * 0.2, 1.0) for m in unique_matches
                ) / keyword_matches

                frequency_weight = min(total_occurrences * 0.1, 0.5)
                strength_boost   = min(specificity_boost + frequency_weight, 0.5)
                final_score      = min(base_score + strength_boost, 1.0)
                category_scores[category] = final_score

        except Exception as error:
            logger.error(f"Keyword scoring error for {category}: {error}")
            continue

    return category_scores

# ============================================================
# WEIGHTED CLASSIFICATION
# ✅ Single source of truth — Redis keywords only
# ✅ No hardcoded rule lists — check_rule_based_overrides removed
# ✅ keyword_scores computed once, reused in override + weighted combo
# ✅ API confidence used instead of hardcoded 0.8
# ✅ Top-3 ML scores scaled by API confidence
# ============================================================
def weighted_category_classification(
    text,
    article_num,
    api_confidence=0.5,
    title="",
    description=""
):
    alpha         = 0.7
    beta          = 0.3
    min_threshold = 0.4

    # ── Step 1: Compute keyword scores ONCE ─────────────────
    # Reused in both override check and weighted combination
    keyword_scores = calculate_keyword_scores(text)

    # ── Step 2: Redis high-confidence override ───────────────
    # ✅ Replaces hardcoded check_rule_based_overrides()
    # If any category has strong keyword signal, skip ML entirely
    if keyword_scores:
        best_kw_cat   = max(keyword_scores, key=keyword_scores.get)
        best_kw_score = keyword_scores[best_kw_cat]

        if best_kw_score >= RULE_CONFIDENCE_THRESHOLD:
            logger.info(
                f"Article {article_num}: REDIS RULE OVERRIDE → {best_kw_cat} "
                f"(keyword_score={best_kw_score:.3f} >= "
                f"threshold={RULE_CONFIDENCE_THRESHOLD})"
            )
            score_breakdown = {
                "ml_score":         0.0,
                "keyword_score":    best_kw_score,
                "combined_score":   best_kw_score,
                "model_confidence": 0.0,
                "api_confidence":   api_confidence,
                "top_3_ml":         {},
                "top_3_combined":   [(best_kw_cat, best_kw_score)],
                "weights":          {"alpha": alpha, "beta": beta},
            }
            fallback_logger.info(
                f"CLASSIFICATION,REDIS_RULE_OVERRIDE,{best_kw_cat},"
                f"{best_kw_score:.3f},0.000,{best_kw_score:.3f},"
                f"0.000,{api_confidence:.3f}"
            )
            return best_kw_cat, best_kw_score, "REDIS_RULE_OVERRIDE", score_breakdown

    # ── Step 3: DistilBART ML classification ────────────────
    # Only runs if no high-confidence keyword override
    distilbart_category, distilbart_model_confidence, distilbart_ml_scores = \
        classify_with_distilbart(text, title, description)

    if distilbart_category and distilbart_ml_scores:
        # ✅ Scale all top-3 scores by API confidence
        ml_scores = {
            cat: score * api_confidence
            for cat, score in distilbart_ml_scores.items()
        }
        logger.info(
            f"Article {article_num}: DistilBART top-3 ML scores "
            f"(scaled by api_conf={api_confidence:.3f}): "
            + ", ".join(f"{k}={v:.3f}" for k, v in ml_scores.items())
        )
    else:
        ml_scores = {}

    # ── Step 4: Weighted combination ────────────────────────
    # ✅ keyword_scores already computed in Step 1 — no recompute
    all_categories = set(
        list(ml_scores.keys()) +
        list(keyword_scores.keys()) +
        CATEGORY_LIST
    )

    category_scores = {}
    for category in all_categories:
        ml_score      = ml_scores.get(category, 0.0)
        keyword_score = keyword_scores.get(category, 0.0)
        combined      = (alpha * ml_score) + (beta * keyword_score)
        category_scores[category] = combined

    if not category_scores:
        logger.warning(
            f"Article {article_num}: NO CLASSIFICATION SCORES - Default: J"
        )
        return "J", 0.2, "DEFAULT_NO_SCORES", {}

    best_category = max(category_scores, key=category_scores.get)
    best_score    = category_scores[best_category]

    score_breakdown = {
        "ml_score":         ml_scores.get(best_category, 0.0),
        "keyword_score":    keyword_scores.get(best_category, 0.0),
        "combined_score":   best_score,
        "model_confidence": distilbart_model_confidence or 0.0,
        "api_confidence":   api_confidence,
        "top_3_ml":         dict(list(ml_scores.items())[:3]),
        "top_3_combined":   sorted(
            category_scores.items(),
            key=lambda x: x[1],
            reverse=True
        )[:3],
        "weights": {"alpha": alpha, "beta": beta},
    }

    # ── Step 5: Check weighted threshold ────────────────────
    if best_score >= min_threshold:
        if ml_scores.get(best_category, 0.0) > 0.6:
            method = "WEIGHTED_ML_DOMINANT"
        elif keyword_scores.get(best_category, 0.0) > 0.7:
            method = "WEIGHTED_KEYWORD_DOMINANT"
        else:
            method = "WEIGHTED_BALANCED"

        logger.info(
            f"Article {article_num}: {method} - "
            f"Category: {best_category}, Score: {best_score:.3f}"
        )
        return best_category, best_score, method, score_breakdown

    # ── Step 6: Final fallback — default J ──────────────────
    logger.warning(
        f"Article {article_num}: DEFAULT J (low confidence: {best_score:.3f})"
    )
    return "J", 0.3, "DEFAULT_LOW_CONFIDENCE", score_breakdown

# ============================================================
# ENTRY POINT
# ============================================================
def enhanced_classify_news_category(text, payload, article_num):
    try:
        title          = payload.get("title",       "")
        description    = payload.get("description", "")
        api_confidence = payload.get("confidence",  0.5)

        category, confidence, method, breakdown = weighted_category_classification(
            text,
            article_num,
            api_confidence=api_confidence,
            title=title,
            description=description,
        )

        logger.info(
            f"Article {article_num}: Classification Breakdown - "
            f"ML: {breakdown.get('ml_score', 0.0):.3f}, "
            f"Keywords: {breakdown.get('keyword_score', 0.0):.3f}, "
            f"Combined: {breakdown.get('combined_score', 0.0):.3f}, "
            f"ModelConf: {breakdown.get('model_confidence', 0.0):.3f}, "
            f"APIConf: {breakdown.get('api_confidence', 0.0):.3f}"
        )

        fallback_logger.info(
            f"CLASSIFICATION,{method},{category},{confidence:.3f},"
            f"{breakdown.get('ml_score', 0.0):.3f},"
            f"{breakdown.get('keyword_score', 0.0):.3f},"
            f"{breakdown.get('model_confidence', 0.0):.3f},"
            f"{breakdown.get('api_confidence', 0.0):.3f}"
        )

        payload["category"] = category
        return category

    except Exception as error:
        logger.error(
            f"Article {article_num}: Enhanced classification error: {error}"
        )
        try:
            category = keyword_based_category(text)
            payload["category"] = category
            logger.warning(
                f"Article {article_num}: FALLBACK to keyword classification: {category}"
            )
            return category
        except Exception as fallback_error:
            logger.error(
                f"Article {article_num}: All classification methods failed: {fallback_error}"
            )
            payload["category"] = "J"
            return "J"