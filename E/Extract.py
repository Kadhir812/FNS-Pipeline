"""
EXTRACT.PY DOCUMENTATION
========================

Purpose:
--------
This script fetches financial news articles from an API, enriches them with NLP (Natural Language Processing), deduplicates them, and sends them to a Kafka stream for further processing.

Key Steps:
----------
1. Fetch news articles from MarketAux API.
2. For each article:
    - Clean the text (remove HTML tags).
    - Classify the article category using a machine learning model (DistilBART) or fallback rules.
    - Extract key phrases and generate a summary using AI (LangChain/Ollama).
    - Aggregate all tickers/entities for identical articles (deduplication).
    - Override sentiment if the summary/content is clearly positive or negative.
3. Only one message per unique article is sent to Kafka, with all associated tickers included.
4. Articles are saved to an archive file for record-keeping.

How Deduplication Works:
------------------------
- Articles with identical content and title are grouped together.
- All tickers/entities mentioned in these articles are collected into lists.
- Only one record per unique article is sent downstream, with all tickers included.

How Sentiment Override Works:
-----------------------------
- If the summary or content contains clear positive words (e.g., "beat", "surge"), the sentiment is set to positive.
- If it contains clear negative words (e.g., "risk", "fall"), the sentiment is set to negative.
- This helps correct misclassifications from the original API/model.

Outputs:
--------
- Each processed article is saved to `news_archive.jsonl` and sent to the Kafka topic `Financenews-raw`.
- Each record includes: title, description, content, summary, key phrases, sentiment, confidence, all tickers/entities, and more.

Who Should Use This:
--------------------
- Anyone who wants to build a financial news pipeline, even with no prior coding or data experience.
- All steps are automated and require no manual intervention.

"""
import requests
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import re
import hashlib
import os
from langchain_ollama import OllamaLLM
from langchain.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
import logging
import sys
import redis
from colorama import Fore, Back, Style, init

# Initialize colorama for cross-platform colored output
init(autoreset=True)

def safe_format(value, format_spec=".2f"):
    """Safely format a value that might be None"""
    if value is None:
        return "None"
    try:
        return f"{float(value):{format_spec}}"
    except (ValueError, TypeError):
        return str(value)

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'Financenews-raw'
MARKETAUX_API_KEY=  'f8mjfUFx1U4xLfJkSxHgJ8wxl6d1pBAMURmyi21N' # <-- Add your API key here

# Path to DistilBART model
DISTILBART_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "T", "models", "distilbart-mnli")
# Flag to use DistilBART for classification if available
USE_DISTILBART = os.path.exists(DISTILBART_PATH)

# Path to FinBERT model for sentiment analysis
FINBERT_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "T", "models", "finbert")
# Flag to use FinBERT for sentiment analysis if available
USE_FINBERT = os.path.exists(FINBERT_PATH)

# Initialize FinBERT model globally (loaded once)
FINBERT_CLASSIFIER = None

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Setup logging with enhanced format for tracking fallbacks
# Create logs directory if it doesn't exist
logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
os.makedirs(logs_dir, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(logs_dir, f"extract_paths_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("extract")

# Add a separate logger for fallback tracking
fallback_logger = logging.getLogger("fallback_tracker")
fallback_handler = logging.FileHandler(os.path.join(logs_dir, f"fallback_usage_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8')
fallback_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
fallback_logger.addHandler(fallback_handler)
fallback_logger.setLevel(logging.INFO)

# Add a separate logger for sentiment analysis tracking
sentiment_logger = logging.getLogger("sentiment_tracker")
sentiment_handler = logging.FileHandler(os.path.join(logs_dir, f"sentiment_analysis_{datetime.now().strftime('%Y%m%d')}.log"), encoding='utf-8')
sentiment_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
sentiment_logger.addHandler(sentiment_handler)
sentiment_logger.setLevel(logging.INFO)

def clean_text(text):
    if text:
        # Remove all XML/HTML tags
        return re.sub(r'<[^>]+>', '', text)
    return ''

def fetch_news():
    url = "https://api.marketaux.com/v1/news/all"
    params = {
        "api_token": MARKETAUX_API_KEY,
        "categories": "finance",
        "language": "en",
        "countries": "us"
    }
    r = requests.get(url, params=params, timeout=10)
    print("DEBUG: Raw API response:")
    print(r.text[:500] + "..." if len(r.text) > 500 else r.text)  # Show first part to avoid console flooding
    try:
        data = r.json()
        # Show entity information from the first article if available
        if data and 'data' in data and len(data['data']) > 0:
            first_article = data['data'][0]
            if 'entities' in first_article and len(first_article['entities']) > 0:
                first_entity = first_article['entities'][0]
                print(f"DEBUG: First entity example - Symbol: {first_entity.get('symbol')}, Name: {first_entity.get('name')}")
    except ValueError:
        print("❌ Not a valid JSON response!")
        print(r.text)  # Check for HTML/XML or error message
        return []
    return data.get("data", [])

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "deepseek-r1:1.5b"

def ollama_query(prompt, text):
    try:
        response = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt.replace('+ text', text)},
            stream=True,
            # timeout=False
        )
        
        if response.status_code != 200:
            print(f"❌ Ollama error: {response.status_code}")
            return "N/A"

        full_reply = ""
        for line in response.iter_lines(decode_unicode=True):
            if line:
                try:
                    data = json.loads(line)
                    full_reply += data.get("response", "")
                except json.JSONDecodeError as e:
                    print(f"❌ Ollama JSON decode error: {e}")
                    continue
        
        # Clean the response - remove <think> blocks and extract clean answer
        cleaned_response = clean_ollama_response(full_reply.strip())
        return cleaned_response if cleaned_response else "N/A"
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Ollama request error: {e}")
        return "N/A"

def clean_ollama_response(response):
    """Clean Ollama response by removing <think> blocks and extracting the actual answer"""
    if not response:
        return "N/A"
    
    # Remove <think>...</think> blocks (DeepSeek-R1 reasoning)
    cleaned = re.sub(r'<think>.*?</think>', '', response, flags=re.DOTALL)
    
    # Remove any remaining XML-like tags
    cleaned = re.sub(r'<[^>]+>', '', cleaned)
    
    # Clean up whitespace and newlines
    cleaned = re.sub(r'\s+', ' ', cleaned).strip()
    
    # If still empty or too long, try to extract meaningful content
    if not cleaned or len(cleaned) > 200:
        # Try to find patterns for each field type
        
        # For categories - look for single letters A-E
        if re.search(r'\b[ABCDE]\b', response):
            category_match = re.search(r'\b([ABCDE])\b', response)
            if category_match:
                return category_match.group(1)
        
        # For impact assessment - look for POSITIVE/NEGATIVE/NEUTRAL
        impact_patterns = ['POSITIVE', 'NEGATIVE', 'NEUTRAL']
        for pattern in impact_patterns:
            if pattern.upper() in response.upper():
                return pattern
        
        # For key phrases - look for comma-separated items
        if ',' in response:
            # Extract text after common prompt words
            after_entities = re.search(r'(?:entities|phrases):\s*(.+)', response, re.IGNORECASE)
            if after_entities:
                phrases = after_entities.group(1).strip()
                # Limit to reasonable length
                if len(phrases) < 500:
                    return phrases
        
        # For summary - look for complete sentences
        sentences = re.findall(r'[A-Z][^.!?]*[.!?]', response)
        if sentences:
            # Return first reasonable sentence
            for sentence in sentences:
                if 5 < len(sentence) < 150:
                    return sentence.strip()
    
    return cleaned if cleaned else "N/A"

def classify_with_distilbart(text):
    """
    Use DistilBART model to classify financial news into categories A-J
    """
    if not USE_DISTILBART or not text or len(text.strip()) < 15:
        logger.info(f"DistilBART skipped: available={USE_DISTILBART}, text_length={len(text) if text else 0}")
        return None
        
    try:
        logger.info("Starting DistilBART classification...")
        start_time = time.time()
        
        # Only import if model is available
        from transformers import pipeline
        
        # Load model
        classifier = pipeline(
            "zero-shot-classification",
            model=DISTILBART_PATH,
            device=-1,  # CPU
            max_length=256,
            truncation=True,
            padding=True
        )
        
        # Truncate text for efficiency
        text_short = text[:200] if len(text) > 200 else text
        
        # 10-category financial news classification
        categories = [
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
            "technology trends and disruption"
        ]
        
        # Category mapping
        category_map = {
            "earnings and quarterly results": "A",         # Company financial results, EPS, revenue, profit
            "analyst ratings and recommendations": "B",    # Analyst upgrades/downgrades, price targets
            "mergers, acquisitions and deals": "C",        # M&A, buyouts, strategic deals
            "regulatory and legal developments": "D",      # Laws, SEC, compliance, government actions
            "economic data and central banks": "E",        # Macro data, Fed, interest rates, inflation
            "corporate actions and leadership": "F",       # Management changes, board, insider trades
            "market trends and sector analysis": "G",      # Sector performance, market sentiment, ETF flows
            "ipo and new listings": "H",                   # IPOs, direct listings, SPACs
            "product launches and innovation": "I",        # New products, R&D, patents, tech launches
            "general business and industry news": "J",     # Misc business news, company updates
            "commodities and energy": "K",                 # Oil, gas, metals, agriculture, energy markets
            "esg and sustainability": "L",                 # ESG, sustainability, climate, governance
            "macro and geopolitics": "M",                  # Geopolitical events, trade wars, global macro
            "technology trends and disruption": "N"        # AI, digital transformation, tech disruption
        }
        
        # Classify with DistilBART
        classification_start = time.time()
        result = classifier(text_short, categories)
        classification_time = time.time() - classification_start
        
        predicted_category = result['labels'][0]
        confidence = result['scores'][0]
        
        # Map to letter code
        final_category = category_map.get(predicted_category, "J")
        
        # Log detailed results
        logger.info(f"DistilBART classification SUCCESS - Category: {final_category} - " 
                    f"Label: '{predicted_category}' - Confidence: {confidence:.3f} - "
                    f"Time: {classification_time:.2f}s")
        
        # Top 3 categories with scores for analysis
        top3 = [(result['labels'][i], result['scores'][i]) for i in range(min(3, len(result['labels'])))]
        logger.debug(f"Top 3 categories: {top3}")
        
        print(f"   🤖 DistilBART Classification: {final_category} ({predicted_category.split()[0]}) - {confidence:.2f}")
        return final_category
        
    except Exception as e:
        logger.error(f"DistilBART classification FAILED: {str(e)}")
        print(f"   ❌ DistilBART error: {e}")
        return None

def load_finbert_classifier():
    """
    Load FinBERT model for sentiment analysis (loaded once globally)
    """
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
            device=-1,  # CPU
            max_length=512,
            truncation=True
        )
        
        logger.info("FinBERT model loaded successfully")
        print(f"   {Fore.GREEN}✅ FinBERT sentiment model loaded{Style.RESET_ALL}")
        return FINBERT_CLASSIFIER
        
    except Exception as e:
        logger.error(f"Failed to load FinBERT model: {str(e)}")
        print(f"   {Fore.RED}❌ FinBERT loading error: {e}{Style.RESET_ALL}")
        return None

def analyze_sentiment_with_finbert(text):
    """
    Analyze sentiment using FinBERT model
    
    Returns:
        tuple: (sentiment_score, confidence) or (None, None) if failed
        sentiment_score: float between -1 (negative) and 1 (positive)
        confidence: float between 0 and 1
    """
    global FINBERT_CLASSIFIER
    
    if not text or len(text.strip()) < 10:
        return None, None
    
    try:
        # Load model if not already loaded
        if FINBERT_CLASSIFIER is None:
            FINBERT_CLASSIFIER = load_finbert_classifier()
        
        if FINBERT_CLASSIFIER is None:
            return None, None
        
        # Truncate text for efficiency
        text_short = text[:512] if len(text) > 512 else text
        
        # Get sentiment prediction
        start_time = time.time()
        result = FINBERT_CLASSIFIER(text_short)[0]
        inference_time = time.time() - start_time
        
        label = result['label'].lower()
        score = result['score']
        
        # Convert FinBERT labels to sentiment score (-1 to 1)
        # FinBERT outputs: positive, negative, neutral
        if label == 'positive':
            sentiment_score = score  # 0 to 1
        elif label == 'negative':
            sentiment_score = -score  # -1 to 0
        else:  # neutral
            sentiment_score = 0.0
        
        logger.info(f"FinBERT sentiment: {label} ({sentiment_score:.3f}), confidence: {score:.3f}, time: {inference_time:.2f}s")
        
        return sentiment_score, score
        
    except Exception as e:
        logger.error(f"FinBERT sentiment analysis failed: {str(e)}")
        return None, None

def compare_and_select_sentiment(marketaux_sentiment, marketaux_confidence, 
                                  finbert_sentiment, finbert_confidence, text, article_num):
    """
    Compare MarketAux and FinBERT sentiment scores and select the most reliable one
    
    Selection criteria:
    1. If both available: choose based on confidence and agreement
    2. If only one available: use that one
    3. If neither available: return None
    
    Returns:
        tuple: (final_sentiment, final_confidence, source_used)
    """
    
    # Case 1: Both available - compare and select
    if marketaux_sentiment is not None and finbert_sentiment is not None:
        # Check if sentiments agree (same direction)
        same_direction = (marketaux_sentiment * finbert_sentiment) >= 0
        
        # Calculate confidence-weighted difference
        sentiment_diff = abs(marketaux_sentiment - finbert_sentiment)
        
        # If they agree and are close, use weighted average
        if same_direction and sentiment_diff < 0.3:
            # Confidence-weighted average
            total_conf = (marketaux_confidence or 0.5) + (finbert_confidence or 0.5)
            weighted_sentiment = (
                (marketaux_sentiment * (marketaux_confidence or 0.5)) +
                (finbert_sentiment * (finbert_confidence or 0.5))
            ) / total_conf
            
            final_confidence = (marketaux_confidence or 0.5) + (finbert_confidence or 0.5)
            final_confidence = min(final_confidence, 1.0)  # Cap at 1.0
            
            sentiment_logger.info(
                f"Article {article_num}: AGREEMENT - Weighted average used - "
                f"MarketAux: {marketaux_sentiment:.3f} ({marketaux_confidence:.3f}), "
                f"FinBERT: {finbert_sentiment:.3f} ({finbert_confidence:.3f}), "
                f"Final: {weighted_sentiment:.3f} ({final_confidence:.3f})"
            )
            print(f"   {Fore.GREEN}🤝 Sentiment: {weighted_sentiment:.3f} (Both agree - weighted avg){Style.RESET_ALL}")
            
            return weighted_sentiment, final_confidence, "HYBRID_AGREEMENT"
        
        # If they disagree or differ significantly, choose higher confidence
        else:
            if (finbert_confidence or 0) > (marketaux_confidence or 0):
                sentiment_logger.info(
                    f"Article {article_num}: DISAGREEMENT - FinBERT selected (higher confidence) - "
                    f"MarketAux: {marketaux_sentiment:.3f} ({marketaux_confidence:.3f}), "
                    f"FinBERT: {finbert_sentiment:.3f} ({finbert_confidence:.3f})"
                )
                print(f"   {Fore.YELLOW}🤖 Sentiment: {finbert_sentiment:.3f} (FinBERT - higher confidence){Style.RESET_ALL}")
                return finbert_sentiment, finbert_confidence, "FINBERT_DOMINANT"
            else:
                sentiment_logger.info(
                    f"Article {article_num}: DISAGREEMENT - MarketAux selected (higher confidence) - "
                    f"MarketAux: {marketaux_sentiment:.3f} ({marketaux_confidence:.3f}), "
                    f"FinBERT: {finbert_sentiment:.3f} ({finbert_confidence:.3f})"
                )
                print(f"   {Fore.CYAN}📊 Sentiment: {marketaux_sentiment:.3f} (MarketAux - higher confidence){Style.RESET_ALL}")
                return marketaux_sentiment, marketaux_confidence, "MARKETAUX_DOMINANT"
    
    # Case 2: Only MarketAux available
    elif marketaux_sentiment is not None:
        sentiment_logger.info(
            f"Article {article_num}: MARKETAUX_ONLY - "
            f"Sentiment: {marketaux_sentiment:.3f} ({marketaux_confidence:.3f})"
        )
        print(f"   {Fore.CYAN}📊 Sentiment: {marketaux_sentiment:.3f} (MarketAux only){Style.RESET_ALL}")
        return marketaux_sentiment, marketaux_confidence, "MARKETAUX_ONLY"
    
    # Case 3: Only FinBERT available (MarketAux fallback)
    elif finbert_sentiment is not None:
        sentiment_logger.info(
            f"Article {article_num}: FINBERT_FALLBACK - "
            f"Sentiment: {finbert_sentiment:.3f} ({finbert_confidence:.3f})"
        )
        print(f"   {Fore.YELLOW}🤖 Sentiment: {finbert_sentiment:.3f} (FinBERT fallback){Style.RESET_ALL}")
        return finbert_sentiment, finbert_confidence, "FINBERT_FALLBACK"
    
    # Case 4: Neither available
    else:
        sentiment_logger.warning(f"Article {article_num}: NO_SENTIMENT - Both sources failed")
        print(f"   {Fore.RED}⚠️ Sentiment: N/A (No sources available){Style.RESET_ALL}")
        return None, None, "NO_SENTIMENT"

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)

# Category constants
CATEGORY_LETTERS = 'ABCDEFGHIJKLMN'
CATEGORY_LIST = list(CATEGORY_LETTERS)  # ['A', 'B', 'C', ..., 'N']

# Load keywords from Redis once at module initialization
keywords_dict = {}
for cat in CATEGORY_LETTERS:
    redis_key = f'category_keywords_{cat}'
    keywords_dict[cat] = set(redis_client.smembers(redis_key))

def keyword_based_category(text):
    text_lower = text.lower()
    for cat, keywords in keywords_dict.items():
        if any(word in text_lower for word in keywords):
            return cat
    return "J"  # Default to general business if no match

def calculate_keyword_scores(text):
    """
    Calculate keyword-based scores for all categories using Redis data
    
    Returns:
        dict: {category: normalized_score}
    """
    text_lower = text.lower()
    category_scores = {}
    
    # Score each category using pre-loaded keywords from Redis
    for category in CATEGORY_LIST:
        try:
            keywords = keywords_dict.get(category, set())
            if keywords:
                keyword_matches = 0
                total_keywords = len(keywords)
                match_weight = 0.0
                
                for keyword in keywords:
                    # Keywords are already strings due to decode_responses=True
                    keyword_lower = keyword.lower()
                    
                    # Count occurrences with different weights
                    if keyword_lower in text_lower:
                        occurrences = text_lower.count(keyword_lower)
                        
                        # Weight longer/more specific keywords higher
                        specificity_weight = min(len(keyword.split()) * 0.2, 1.0)
                        frequency_weight = min(occurrences * 0.3, 1.0)
                        
                        match_weight += (1.0 + specificity_weight + frequency_weight)
                        keyword_matches += 1
                
                # Normalize score (0-1 range)
                if total_keywords > 0:
                    # Base score from match ratio
                    base_score = keyword_matches / total_keywords
                    
                    # Boost from match strength  
                    strength_boost = min(match_weight / total_keywords, 0.5)
                    
                    # Final normalized score
                    final_score = min(base_score + strength_boost, 1.0)
                    category_scores[category] = final_score
                    
        except Exception as e:
            logger.error(f"🔴 Redis keyword scoring error for {category}: {e}")
            continue
    
    return category_scores

def check_rule_based_overrides(text):
    """
    Check for specific rule-based overrides that should take precedence
    """
    text_lower = text.lower()
    
    # High-confidence rule overrides
    if any(term in text_lower for term in ['insider trading', 'insider trade', 'insider sold', 'insider bought']):
        return 'F'  # Corporate Actions & Leadership
    
    if any(term in text_lower for term in ['ipo filing', 'ipo launch', 'going public', 'initial public offering']):
        return 'H'  # IPO & New Listings
        
    if any(term in text_lower for term in ['merger agreement', 'acquisition deal', 'takeover bid']):
        return 'C'  # M&A
        
    if any(term in text_lower for term in ['sec filing', 'sec investigation', 'regulatory probe']):
        return 'D'  # Regulatory
        
    if any(term in text_lower for term in ['fed meeting', 'interest rate decision', 'monetary policy']):
        return 'E'  # Economic Data & Fed
    
    return None

def weighted_category_classification(text, article_num):
    """
    Combined weighted scoring using DistilBART + Redis keywords
    
    Returns:
        tuple: (final_category, final_confidence, method_used, score_breakdown)
    """
    
    # Configuration weights (tunable based on performance)
    ALPHA = 0.7    # ML weight (DistilBART)
    BETA = 0.3     # Keywords weight (Redis)
    MIN_THRESHOLD = 0.4  # Minimum combined confidence
    
    # Initialize scores dictionary for all categories
    category_scores = {}
    
    # Step 1: Get DistilBART scores
    ml_scores = {}
    distilbart_category = classify_with_distilbart(text)
    distilbart_confidence = 0.8  # Default confidence
    
    if distilbart_category:
        ml_scores[distilbart_category] = distilbart_confidence
    
    # Step 2: Get Redis keyword scores
    keyword_scores = calculate_keyword_scores(text)
    
    # Step 3: Combine weighted scores
    all_categories = set(list(ml_scores.keys()) + list(keyword_scores.keys()) + ['A','B','C','D','E','F','G','H','I','J','K','L','M','N'])
    
    for category in all_categories:
        ml_score = ml_scores.get(category, 0.0)
        keyword_score = keyword_scores.get(category, 0.0)
        
        # Weighted combination
        combined_score = (ALPHA * ml_score) + (BETA * keyword_score)
        category_scores[category] = combined_score
    
    # Step 4: Find best category
    if category_scores:
        best_category = max(category_scores, key=category_scores.get)
        best_score = category_scores[best_category]
        
        # Score breakdown for logging only
        score_breakdown = {
            'ml_score': ml_scores.get(best_category, 0.0),
            'keyword_score': keyword_scores.get(best_category, 0.0),
            'combined_score': best_score,
            'weights': {'alpha': ALPHA, 'beta': BETA},
            'top_3_combined': sorted(category_scores.items(), key=lambda x: x[1], reverse=True)[:3]
        }
        
        # Determine method used and apply thresholds
        if best_score >= MIN_THRESHOLD:
            if ml_scores.get(best_category, 0.0) > 0.6:
                method = "WEIGHTED_ML_DOMINANT"
                color = Fore.GREEN
            elif keyword_scores.get(best_category, 0.0) > 0.7:
                method = "WEIGHTED_KEYWORD_DOMINANT" 
                color = Fore.CYAN
            else:
                method = "WEIGHTED_BALANCED"
                color = Fore.YELLOW
                
            logger.info(f"Article {article_num}: {method} - Category: {best_category}, Score: {best_score:.3f}")
            print(f"   {color}✅ Category: {best_category} (Weighted - {best_score:.3f}){Style.RESET_ALL}")
            return best_category, best_score, method, score_breakdown
        else:
            # Fallback to rule-based overrides
            rule_category = check_rule_based_overrides(text)
            if rule_category:
                logger.info(f"Article {article_num}: RULE OVERRIDE - Category: {rule_category}, Low weighted score: {best_score:.3f}")
                print(f"   {Fore.MAGENTA}⚡ Override: {rule_category} (Rule-based){Style.RESET_ALL}")
                return rule_category, 0.8, "RULE_OVERRIDE", score_breakdown
            else:
                # Default category
                logger.warning(f"Article {article_num}: DEFAULT CATEGORY - J, Low confidence: {best_score:.3f}")
                print(f"   {Fore.RED}⚠️ Category: J (Default - low confidence){Style.RESET_ALL}")
                return 'J', 0.3, "DEFAULT_LOW_CONFIDENCE", score_breakdown
    else:
        # No scores available - default
        logger.warning(f"Article {article_num}: NO CLASSIFICATION SCORES - Default: J")
        print(f"   {Fore.RED}❌ Category: J (No scores available){Style.RESET_ALL}")
        return 'J', 0.2, "DEFAULT_NO_SCORES", {}

def enhanced_classify_news_category(text, payload, article_num):
    """
    Enhanced classification using weighted/combined scoring
    Returns only the final category in payload
    """
    
    try:
        # Use weighted classification
        category, confidence, method, breakdown = weighted_category_classification(text, article_num)
        
        # Log detailed breakdown for analysis (but don't add to payload)
        logger.info(f"Article {article_num}: Classification Breakdown - ML: {breakdown.get('ml_score', 0.0):.3f}, "
                   f"Keywords: {breakdown.get('keyword_score', 0.0):.3f}, Combined: {breakdown.get('combined_score', 0.0):.3f}")
        
        # Log to fallback tracking file
        fallback_logger.info(f"CLASSIFICATION,{method},{category},{confidence:.3f},{breakdown.get('ml_score', 0.0):.3f},{breakdown.get('keyword_score', 0.0):.3f}")
        
        # Update payload with ONLY the final category
        payload['category'] = category
        
        return category
        
    except Exception as e:
        logger.error(f"Article {article_num}: Enhanced classification error: {e}")
        # Fallback to simple keyword classification
        try:
            category = keyword_based_category(text)
            payload['category'] = category
            logger.warning(f"Article {article_num}: FALLBACK to keyword classification: {category}")
            print(f"   {Fore.CYAN}✅ Category: {category} (Keyword fallback){Style.RESET_ALL}")
            return category
        except Exception as e2:
            logger.error(f"Article {article_num}: All classification methods failed: {e2}")
            payload['category'] = 'J'
            print(f"   {Fore.RED}❌ Category: J (All methods failed){Style.RESET_ALL}")
            return 'J'

def enrich_news(payload, article_num, total_articles):
    # Sentiment override logic for clear positive/negative summaries
    summary = payload.get('summary', '').lower()
    content = payload.get('content', '').lower()
    # If summary/content contains clear positive/negative phrases, override sentiment
    positive_keywords = ["beat", "surge", "growth", "bullish", "strong", "record", "positive", "rally", "gain"]
    negative_keywords = ["risk", "fall", "drop", "loss", "bearish", "struggle", "decline", "negative", "warning"]
    if any(word in summary or word in content for word in positive_keywords):
        payload['sentiment'] = max(payload.get('sentiment', 0.0) or 0.0, 0.7)
    elif any(word in summary or word in content for word in negative_keywords):
        payload['sentiment'] = min(payload.get('sentiment', 0.0) or 0.0, -0.7)
    text = clean_text(payload.get('content', '') or payload.get('description', ''))
    
    if not text:
        print(f"   ⚠️  No text content available for processing")
        text = payload.get('title', '')
    
    print(f"\n🔄 Processing Article {article_num}/{total_articles}")
    print(f"   📰 Title: {payload.get('title', 'N/A')[:80]}...")
    print(f"   📝 Text Length: {len(text)} chars")
    
    start_time = time.time()

    # CATEGORY - Enhanced ML-based classification with weighted scoring
    print(f"   � Classifying news category...")
    enhanced_classify_news_category(text, payload, article_num)

    # Use LangChain for key phrases and summary (keep as is)
    if USE_LANGCHAIN:
        try:
            print(f"   {Fore.BLUE}🔄 Processing with LangChain...{Style.RESET_ALL}")
            logger.info(f"Article {article_num}: Using LangChain path")
            
            # Process the text with our sequential chain
            start_langchain = time.time()
            results = NEWS_PROCESSING_CHAIN.invoke({"text": text})
            langchain_time = time.time() - start_langchain
            
            # Extract results
            payload['key_phrases'] = clean_ollama_response(results.get('key_phrases', 'N/A'))
            print(f"   {Fore.GREEN}✅ Key phrases: {payload['key_phrases'][:50]}... (LangChain){Style.RESET_ALL}")
            
            payload['summary'] = clean_ollama_response(results.get('summary', 'Financial news update'))
            print(f"   {Fore.GREEN}✅ Summary: {payload['summary'][:60]}... (LangChain){Style.RESET_ALL}")
            
            logger.info(f"Article {article_num}: LangChain SUCCESS - Time: {langchain_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Article {article_num}: LangChain FAILED - Error: {str(e)}")
            fallback_logger.warning(f"PROCESSING_FALLBACK - Article: {article_num}, Reason: LangChain_Error, Method: Direct_Ollama, Error: {str(e)}")
            print(f"   {Fore.RED}❌ LangChain processing error: {e}{Style.RESET_ALL}")
            print(f"   {Fore.YELLOW}🔄 Falling back to individual Ollama calls...{Style.RESET_ALL}")
            # Fall back to the original processing method for key phrases and summary
            fallback_to_individual_processing(payload, text, article_num)
    else:
        logger.info(f"Article {article_num}: Using direct Ollama path (LangChain not available)")
        fallback_logger.info(f"PROCESSING_DIRECT - Article: {article_num}, Reason: LangChain_Unavailable, Method: Direct_Ollama")
        print(f"   {Fore.CYAN}🔄 Using direct Ollama calls (LangChain unavailable){Style.RESET_ALL}")
        # Use the original individual processing methods for key phrases and summary
        fallback_to_individual_processing(payload, text, article_num)


    
    processing_time = time.time() - start_time
    print(f"   ⏱️  Processing time: {processing_time:.2f}s")
    print(f"   🎉 Article {article_num}/{total_articles} COMPLETED!")
    
    return payload

def fallback_to_individual_processing(payload, text, article_num):
    """Fallback to individual Ollama calls for key phrases and summary only"""
    logger.warning(f"Article {article_num}: Using FALLBACK path with direct Ollama calls")
    
    # KEY PHRASES - Advanced NER with financial focus
    print(f"   {Fore.CYAN}🔍 Extracting key phrases... (Direct Ollama){Style.RESET_ALL}")
    try:
        key_phrases_prompt = (
            "Extract financial entities: tickers, amounts, percentages, companies, dates, actions.\n"
            "Format: lowercase, comma-separated, max 15 items.\n"
            "Text: + text\n"
            "Entities:"
        )
        start_time = time.time()
        payload['key_phrases'] = ollama_query(key_phrases_prompt, text)
        process_time = time.time() - start_time
        logger.info(f"Article {article_num}: Direct Ollama - Key phrases SUCCESS - Time: {process_time:.2f}s")
        print(f"   {Fore.CYAN}✅ Key phrases: {payload['key_phrases'][:50]}... (Direct Ollama - {process_time:.2f}s){Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"Article {article_num}: Direct Ollama - Key phrases FAILED: {str(e)}")
        print(f"   {Fore.RED}❌ Key phrases error: {e}{Style.RESET_ALL}")
        payload['key_phrases'] = "N/A"

    # SUMMARY - Constraint-based generation
    print(f"   {Fore.CYAN}📋 Generating summary... (Direct Ollama){Style.RESET_ALL}")
    try:
        summary_prompt = (
            "One sentence summary: Subject + Action + Impact (<15 words).\n"
            "Text: + text\n"
            "Summary:"
        )
        start_time = time.time()
        payload['summary'] = ollama_query(summary_prompt, text)
        process_time = time.time() - start_time
        logger.info(f"Article {article_num}: Direct Ollama - Summary SUCCESS - Time: {process_time:.2f}s")
        print(f"   {Fore.CYAN}✅ Summary: {payload['summary'][:60]}... (Direct Ollama - {process_time:.2f}s){Style.RESET_ALL}")
    except Exception as e:
        logger.error(f"Article {article_num}: Direct Ollama - Summary FAILED: {str(e)}")
        print(f"   {Fore.RED}❌ Summary error: {e}{Style.RESET_ALL}")
        payload['summary'] = "Financial news update"

    # Impact assessment will be calculated in the main function using MarketAux data

def get_doc_id(article):
    # Use title + content for uniqueness (or title + publishedAt if you prefer)
    key = (article.get('title', '') or '') + (article.get('snippet', '') or '')
    return hashlib.sha256(key.encode('utf-8')).hexdigest()

def load_existing_doc_ids(path):
    doc_ids = set()
    try:
        with open(path, 'r', encoding='utf-8') as f:
            for line in f:
                try:
                    obj = json.loads(line)
                    if 'doc_id' in obj:
                        doc_ids.add(obj['doc_id'])
                except Exception:
                    continue
    except FileNotFoundError:
        pass
    return doc_ids

# Initialize Ollama model
def get_ollama_model():
    return OllamaLLM(model=OLLAMA_MODEL, base_url="http://localhost:11434")

# Create the LangChain processing pipeline using modern patterns
def create_news_processing_chain():
    # Initialize the LLM
    llm = get_ollama_model()
    output_parser = StrOutputParser()
    
    # KEY PHRASES CHAIN
    key_phrases_template = """Extract financial entities: tickers, amounts, percentages, companies, dates, actions.
Format: lowercase, comma-separated, max 15 items.

Text: {text}

Entities:"""
    
    key_phrases_prompt = PromptTemplate(
        template=key_phrases_template,
        input_variables=["text"]
    )
    
    key_phrases_chain = key_phrases_prompt | llm | output_parser
    
    # SUMMARY CHAIN
    summary_template = """One sentence summary: Subject + Action + Impact (<15 words).

Text: {text}

Summary:"""
    
    summary_prompt = PromptTemplate(
        template=summary_template,
        input_variables=["text"]
    )
    
    summary_chain = summary_prompt | llm | output_parser
    

    
    # Create a properly chainable object instead of a function
    from langchain_core.runnables import RunnableParallel, RunnableLambda
    
    # Define a function to process with individual chains
    def process_chains(inputs):
        text = inputs["text"]
        results = {}
        
        # Run key phrases chain
        results["key_phrases"] = key_phrases_chain.invoke({"text": text})
        
        # Run summary chain
        results["summary"] = summary_chain.invoke({"text": text})
        

        
        return results
    
    # Return a chainable object
    return RunnableLambda(process_chains)

# Initialize the chain once - add this after the create_news_processing_chain function
try:
    NEWS_PROCESSING_CHAIN = create_news_processing_chain()
    USE_LANGCHAIN = True
    print(f"{Fore.GREEN}✅ LangChain processing chain initialized successfully{Style.RESET_ALL}")
except Exception as e:
    USE_LANGCHAIN = False
    print(f"{Fore.YELLOW}⚠️ LangChain initialization failed: {e}{Style.RESET_ALL}")
    print(f"{Fore.YELLOW}⚠️ Falling back to direct Ollama calls{Style.RESET_ALL}")


def main():
    archive_path = 'news_archive.jsonl'
    
    # Indicate classification and processing methods with colors
    print(f"\n{Fore.MAGENTA}🤖 EXTRACT.PY INITIALIZATION{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*50}{Style.RESET_ALL}")
    
    if USE_DISTILBART:
        print(f"{Fore.GREEN}🤖 ✅ DistilBART ENABLED for news classification{Style.RESET_ALL}")
        print(f"   📁 Model path: {DISTILBART_PATH}")
        logger.info("INITIALIZATION: DistilBART classification ENABLED")
    else:
        print(f"{Fore.YELLOW}📝 ⚠️  DistilBART NOT FOUND - Using keyword fallback only{Style.RESET_ALL}")
        logger.warning("INITIALIZATION: DistilBART classification DISABLED - keyword fallback only")
    
    if USE_FINBERT:
        print(f"{Fore.GREEN}🤖 ✅ FinBERT ENABLED for sentiment analysis{Style.RESET_ALL}")
        print(f"   📁 Model path: {FINBERT_PATH}")
        logger.info("INITIALIZATION: FinBERT sentiment analysis ENABLED")
        # Pre-load FinBERT model
        load_finbert_classifier()
    else:
        print(f"{Fore.YELLOW}📝 ⚠️  FinBERT NOT FOUND - Using MarketAux sentiment only{Style.RESET_ALL}")
        logger.warning("INITIALIZATION: FinBERT sentiment analysis DISABLED - MarketAux only")
        
    if USE_LANGCHAIN:
        print(f"{Fore.GREEN}⚡ ✅ LangChain ENABLED for sequential processing pipeline{Style.RESET_ALL}")
        logger.info("INITIALIZATION: LangChain processing pipeline ENABLED")
    else:
        print(f"{Fore.YELLOW}🔄 ⚠️  LangChain NOT AVAILABLE - Using direct Ollama calls{Style.RESET_ALL}")
        logger.warning("INITIALIZATION: LangChain DISABLED - direct Ollama fallback")
    
    print(f"{Fore.BLUE}📊 Redis keywords loaded for {len(keywords_dict)} categories{Style.RESET_ALL}")
    print(f"{Fore.MAGENTA}{'='*50}{Style.RESET_ALL}\n")
        
    # Rest of your main function remains the same...
    while True:
        try:
            print(f"\n🚀 Starting new batch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            articles = fetch_news()
            print(f"📥 Fetched {len(articles)} articles from API")

            existing_doc_ids = load_existing_doc_ids(archive_path)
            print(f"📚 Found {len(existing_doc_ids)} existing articles in archive")

            new_articles = 0
            skipped_articles = 0
            total_start_time = time.time()

            # Deduplicate by doc_id and aggregate symbols/entities
            dedup_map = {}
            for i, article in enumerate(articles, 1):
                publish_date = article.get('published_at')
                if isinstance(publish_date, datetime):
                    publish_date = publish_date.isoformat()

                doc_id = get_doc_id(article)
                if doc_id in existing_doc_ids:
                    print(f"\n⏭️  Skipping duplicate {i}/{len(articles)}: {article.get('title', '')[:50]}...")
                    skipped_articles += 1
                    continue

                # Aggregate all symbols/entities for this doc_id
                entities = article.get('entities', [])
                symbols = []
                entity_names = []
                sentiments = []
                confidences = []
                for entity in entities:
                    symbols.append(entity.get('symbol'))
                    entity_names.append(entity.get('name'))
                    sentiments.append(entity.get('sentiment_score'))
                    confidences.append(entity.get('match_score'))

                # Get MarketAux sentiment (use first entity for main fields, but keep all in lists)
                marketaux_sentiment = sentiments[0] if sentiments else None
                marketaux_confidence = confidences[0] if confidences else None

                # Get text for FinBERT analysis
                article_text = clean_text(article.get('snippet', '') or article.get('description', '') or article.get('title', ''))
                
                # Analyze sentiment with FinBERT
                print(f"\n🔬 Analyzing sentiment for article {i}/{len(articles)}...")
                finbert_sentiment, finbert_confidence = analyze_sentiment_with_finbert(article_text)
                
                # Compare and select best sentiment
                final_sentiment, final_confidence, sentiment_source = compare_and_select_sentiment(
                    marketaux_sentiment, marketaux_confidence,
                    finbert_sentiment, finbert_confidence,
                    article_text, i
                )

                payload = {
                    'doc_id': doc_id,
                    'title': article.get('title', ''),
                    'description': article.get('description', ''),
                    'content': article.get('snippet', ''),
                    'sentiment': final_sentiment,
                    'confidence': final_confidence,
                    'sentiment_source': sentiment_source,  # Track which method was used
                    'publishedAt': publish_date,
                    'source': article.get('source', ''),
                    'link': article.get('url', ''),
                    'image_url': article.get('image_url', None),
                    'symbols': symbols,
                    'entity_names': entity_names,
                }

                # NLP enrichment with detailed debugging
                payload = enrich_news(payload, i, len(articles))

                # Sentiment override already handled in enrich_news

                dedup_map[doc_id] = payload

            # Write deduplicated articles to archive and Kafka
            with open(archive_path, 'a', encoding='utf-8') as f:
                for payload in dedup_map.values():
                    f.write(json.dumps(payload, ensure_ascii=False) + '\n')
                    producer.send(TOPIC, value=payload)
                    new_articles += 1

            producer.flush()

            total_time = time.time() - total_start_time
            total_minutes = total_time / 60  # Convert seconds to minutes

            print(f"\n{Fore.MAGENTA}📊 BATCH SUMMARY:{Style.RESET_ALL}")
            print(f"   📥 Total articles fetched: {len(articles)}")
            print(f"   {Fore.GREEN}✅ New articles processed: {new_articles}{Style.RESET_ALL}")
            print(f"   {Fore.YELLOW}⏭️  Duplicate articles skipped: {skipped_articles}{Style.RESET_ALL}")
            print(f"   ⏱️  Total batch time: {total_minutes:.2f} minutes ({total_time:.2f} seconds)")
            print(f"   ⚡ Average time per article: {(total_time/max(new_articles, 1))/60:.2f} minutes ({total_time/max(new_articles, 1):.2f} seconds)")
            
            # Add method usage summary (can be enhanced further with counters)
            if USE_DISTILBART:
                print(f"   {Fore.GREEN}🤖 Classification: DistilBART + keyword fallback{Style.RESET_ALL}")
            else:
                print(f"   {Fore.CYAN}📝 Classification: Keyword-based only{Style.RESET_ALL}")
            
            if USE_FINBERT:
                print(f"   {Fore.GREEN}💚 Sentiment: Hybrid (FinBERT + MarketAux comparison){Style.RESET_ALL}")
            else:
                print(f"   {Fore.CYAN}📊 Sentiment: MarketAux API only{Style.RESET_ALL}")
                
            if USE_LANGCHAIN:
                print(f"   {Fore.GREEN}⚡ Processing: LangChain pipeline + Ollama fallback{Style.RESET_ALL}")
            else:
                print(f"   {Fore.CYAN}🔄 Processing: Direct Ollama calls only{Style.RESET_ALL}")
            
            print(f"   {Fore.BLUE}�💤 Sleeping for 10 minutes...{Style.RESET_ALL}")
            time.sleep(600)

        except Exception as e:
            print(f"❌ Batch Error: {e}")
            print(f"   🔄 Retrying in 60 seconds...")
            time.sleep(60)

if __name__ == '__main__':
    main()