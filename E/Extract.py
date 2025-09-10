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
MARKETAUX_API_KEY=  'qyntLjkPLqpWNaiP64UufWDwnWdQsp1aLNh9p3sw' # <-- Add your API key here

# Path to DistilBART model
DISTILBART_PATH = os.path.join(os.path.dirname(os.path.dirname(__file__)), "T", "models", "distilbart-mnli")
# Flag to use DistilBART for classification if available
USE_DISTILBART = os.path.exists(DISTILBART_PATH)

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
        logging.FileHandler(os.path.join(logs_dir, f"extract_paths_{datetime.now().strftime('%Y%m%d')}.log")),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("extract")

# Add a separate logger for fallback tracking
fallback_logger = logging.getLogger("fallback_tracker")
fallback_handler = logging.FileHandler(os.path.join(logs_dir, f"fallback_usage_{datetime.now().strftime('%Y%m%d')}.log"))
fallback_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
fallback_logger.addHandler(fallback_handler)
fallback_logger.setLevel(logging.INFO)

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
            timeout=90
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

redis_client = redis.Redis(host='localhost', port=6379, decode_responses=True)
category_letters = 'ABCDEFGHIJKLMN'
keywords_dict = {}
for cat in category_letters:
    redis_key = f'category_keywords_{cat}'
    keywords_dict[cat] = set(redis_client.smembers(redis_key))

def keyword_based_category(text):
    text_lower = text.lower()
    for cat, keywords in keywords_dict.items():
        if any(word in text_lower for word in keywords):
            return cat
    return "J"  # Default to general business if no match

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

    # CATEGORY - ML-based classification with DistilBART
    print(f"   📂 Classifying news category...")
    distilbart_category = classify_with_distilbart(text)
    distilbart_confidence = payload.get('confidence', 1.0) or 1.0

    # Confidence-based fallback with enhanced logging
    if distilbart_category and distilbart_confidence >= 0.3:
        # === RULES-BASED OVERRIDES FOR COMMON MISLABELS ===
        if distilbart_category == "C" and re.search(r'\binsider trade\b', text.lower()):
            payload['category'] = "F"
            logger.info(f"Article {article_num}: RULE OVERRIDE - Insider trade → F (corporate actions)")
            print(f"   {Fore.YELLOW}🔄 Override: Insider trade → F (corporate actions){Style.RESET_ALL}")
        elif distilbart_category == "H" and not re.search(r'\bipo\b|\bnew listing\b|\bpublic offering\b', text.lower()):
            payload['category'] = "J"
            logger.info(f"Article {article_num}: RULE OVERRIDE - Non-IPO news → J (general business)")
            print(f"   {Fore.YELLOW}🔄 Override: Non-IPO news → J (general business){Style.RESET_ALL}")
        elif distilbart_category in ["J", "H"] and re.search(r'\betf\b|\bperformance\b|\bsector\b|\btrend\b', text.lower()):
            payload['category'] = "G"
            logger.info(f"Article {article_num}: RULE OVERRIDE - ETF/stock performance → G (market trends)")
            print(f"   {Fore.YELLOW}🔄 Override: ETF/stock performance → G (market trends){Style.RESET_ALL}")
        else:
            payload['category'] = distilbart_category
            logger.info(f"Article {article_num}: DISTILBART SUCCESS - Category: {payload['category']}, Confidence: {distilbart_confidence:.3f}")
            print(f"   {Fore.GREEN}✅ Category: {payload['category']} (DistilBART - {distilbart_confidence:.2f}){Style.RESET_ALL}")
    else:
        # Layered fallback: use Redis keywords for all categories
        try:
            payload['category'] = keyword_based_category(text)
            fallback_reason = "Low confidence" if distilbart_category else "DistilBART unavailable"
            logger.warning(f"Article {article_num}: KEYWORD FALLBACK - Reason: {fallback_reason}, Category: {payload['category']}")
            fallback_logger.warning(f"CLASSIFICATION_FALLBACK - Article: {article_num}, Reason: {fallback_reason}, Method: Redis_Keywords, Category: {payload['category']}")
            print(f"   {Fore.CYAN}✅ Category: {payload['category']} (Keyword fallback - {fallback_reason}){Style.RESET_ALL}")
        except Exception as e:
            logger.error(f"Article {article_num}: CLASSIFICATION FAILED - DistilBART and Keywords both failed: {str(e)}")
            fallback_logger.error(f"CLASSIFICATION_FAILURE - Article: {article_num}, Error: {str(e)}, Method: Default_Assignment")
            print(f"   {Fore.RED}❌ Category classification error: {e}{Style.RESET_ALL}")
            print(f"   {Fore.MAGENTA}🔄 Using default category: J (general business){Style.RESET_ALL}")
            payload['category'] = "J"

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

def keyword_based_category(text):
    text_lower = text.lower()
    for cat, keywords in keywords_dict.items():
        if any(word in text_lower for word in keywords):
            return cat
    return "J"  # Default to general business if no match

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

                # Use first entity for main fields, but keep all in lists
                sentiment = sentiments[0] if sentiments else None
                confidence = confidences[0] if confidences else None
                symbol = symbols[0] if symbols else None
                entity_name = entity_names[0] if entity_names else None

                payload = {
                    'doc_id': doc_id,
                    'title': article.get('title', ''),
                    'description': article.get('description', ''),
                    'content': article.get('snippet', ''),
                    'sentiment': sentiment,
                    'confidence': confidence,
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
                print(f"   {Fore.CYAN}� Classification: Keyword-based only{Style.RESET_ALL}")
                
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
