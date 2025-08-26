import requests
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import re
import hashlib
import os
# Modern LangChain imports
from langchain_ollama import OllamaLLM
from langchain.prompts import PromptTemplate
from langchain_core.runnables import RunnablePassthrough
from langchain_core.output_parsers import StrOutputParser
import logging
import sys

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

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(f"extract_paths_{datetime.now().strftime('%Y%m%d')}.log"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("extract")

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
            "earnings and quarterly results",           # A = EARNINGS
            "analyst ratings and recommendations",      # B = ANALYST-RATINGS  
            "mergers acquisitions and deals",          # C = M&A
            "regulatory and legal developments",        # D = REGULATORY
            "economic data and federal reserve",        # E = ECONOMIC-DATA
            "corporate actions and leadership",         # F = CORPORATE-ACTIONS
            "market trends and sector analysis",        # G = MARKET-TRENDS
            "ipo and new listings",                    # H = IPO-LISTINGS
            "product launches and innovation",          # I = PRODUCT-NEWS
            "general business and industry news"        # J = GENERAL-BUSINESS
        ]
        
        # Category mapping
        category_map = {
            "earnings and quarterly results": "A",
            "analyst ratings and recommendations": "B",
            "mergers acquisitions and deals": "C",
            "regulatory and legal developments": "D",
            "economic data and federal reserve": "E",
            "corporate actions and leadership": "F",
            "market trends and sector analysis": "G",
            "ipo and new listings": "H",
            "product launches and innovation": "I",
            "general business and industry news": "J"
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

def enrich_news(payload, article_num, total_articles):
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
    if distilbart_category:
        payload['category'] = distilbart_category
        print(f"   ✅ Category: {payload['category']}")
    else:
        # Fallback to rule-based classification if DistilBART fails
        try:
            # Simple rule-based classification for fallback
            if re.search(r'\b(earnings|revenue|profit|quarterly)\b', text.lower()):
                payload['category'] = "A"
            elif re.search(r'\b(upgrade|downgrade|rating|analyst)\b', text.lower()):
                payload['category'] = "B"
            elif re.search(r'\b(merger|acquisition|deal)\b', text.lower()):
                payload['category'] = "C"
            elif re.search(r'\b(regulation|sec|compliance|policy)\b', text.lower()):
                payload['category'] = "D"
            else:
                payload['category'] = "J"  # Default category
            
            print(f"   ✅ Category (fallback): {payload['category']}")
        except Exception as e:
            print(f"   ❌ Category classification error: {e}")
            payload['category'] = "J"

    # Use LangChain for key phrases and summary (keep as is)
    if USE_LANGCHAIN:
        try:
            print("   🔄 Processing with LangChain...")
            logger.info(f"Article {article_num}: Using LangChain path")
            
            # Process the text with our sequential chain
            start_langchain = time.time()
            results = NEWS_PROCESSING_CHAIN.invoke({"text": text})
            langchain_time = time.time() - start_langchain
            
            # Extract results
            payload['key_phrases'] = clean_ollama_response(results.get('key_phrases', 'N/A'))
            print(f"   ✅ Key phrases: {payload['key_phrases'][:50]}...")
            
            payload['summary'] = clean_ollama_response(results.get('summary', 'Financial news update'))
            print(f"   ✅ Summary: {payload['summary'][:60]}...")
            
            logger.info(f"Article {article_num}: LangChain SUCCESS - Time: {langchain_time:.2f}s")
            
        except Exception as e:
            logger.error(f"Article {article_num}: LangChain FAILED - Error: {str(e)}")
            print(f"   ❌ LangChain processing error: {e}")
            print("   🔄 Falling back to individual Ollama calls...")
            # Fall back to the original processing method for key phrases and summary
            fallback_to_individual_processing(payload, text)
    else:
        logger.info(f"Article {article_num}: Using direct Ollama path (LangChain not available)")
        # Use the original individual processing methods for key phrases and summary
        fallback_to_individual_processing(payload, text)

    # Apply impact assessment rules using MarketAux API's sentiment/confidence
    print(f"   📊 Calculating impact assessment from MarketAux sentiment...")
    impact = calculate_impact_assessment(
        sentiment=payload.get('sentiment'),  # Direct from MarketAux API
        confidence=payload.get('confidence'),  # Direct from MarketAux API
        risk_score=payload.get('risk_score')
    )
    payload['impact_assessment'] = impact
    print(f"   ✅ Impact: {impact} (from sentiment={payload.get('sentiment'):.2f}, confidence={payload.get('confidence'):.1f})")
    
    processing_time = time.time() - start_time
    print(f"   ⏱️  Processing time: {processing_time:.2f}s")
    print(f"   🎉 Article {article_num}/{total_articles} COMPLETED!")
    
    return payload

def fallback_to_individual_processing(payload, text):
    """Fallback to individual Ollama calls for key phrases and summary only"""
    logger.info("Using fallback path with direct Ollama calls")
    
    # KEY PHRASES - Advanced NER with financial focus
    print(f"   🔍 Extracting key phrases...")
    try:
        key_phrases_prompt = (
            "Extract financial entities: tickers, amounts, percentages, companies, dates, actions.\n"
            "Format: lowercase, comma-separated, max 15 items.\n"
            "Text: + text\n"
            "Entities:"
        )
        start_time = time.time()
        payload['key_phrases'] = ollama_query(key_phrases_prompt, text)
        logger.info(f"Direct Ollama - Key phrases SUCCESS - Time: {time.time() - start_time:.2f}s")
        print(f"   ✅ Key phrases: {payload['key_phrases'][:50]}...")
    except Exception as e:
        logger.error(f"Direct Ollama - Key phrases FAILED: {str(e)}")
        print(f"   ❌ Key phrases error: {e}")
        payload['key_phrases'] = "N/A"

    # SUMMARY - Constraint-based generation
    print(f"   📋 Generating summary...")
    try:
        summary_prompt = (
            "One sentence summary: Subject + Action + Impact (<15 words).\n"
            "Text: + text\n"
            "Summary:"
        )
        start_time = time.time()
        payload['summary'] = ollama_query(summary_prompt, text)
        logger.info(f"Direct Ollama - Summary SUCCESS - Time: {time.time() - start_time:.2f}s")
        print(f"   ✅ Summary: {payload['summary'][:60]}...")
    except Exception as e:
        logger.error(f"Direct Ollama - Summary FAILED: {str(e)}")
        print(f"   ❌ Summary error: {e}")
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
    
    # IMPACT CHAIN
    impact_template = """Market impact: POSITIVE/NEGATIVE/NEUTRAL
Consider: earnings, ratings, M&A, regulations, guidance.

Text: {text}

Impact:"""
    
    impact_prompt = PromptTemplate(
        template=impact_template,
        input_variables=["text"]
    )
    
    impact_chain = impact_prompt | llm | output_parser
    
    # Create a chain that returns a dictionary with all the results
    def process_all(inputs):
        text = inputs["text"]
        return {
            "key_phrases": key_phrases_chain.invoke({"text": text}),
            "summary": summary_chain.invoke({"text": text}),
            "impact_assessment": impact_chain.invoke({"text": text})
        }
    
    # Return a simple function that processes everything
    return lambda inputs: process_all(inputs)

# Initialize the chain once - add this after the create_news_processing_chain function
try:
    NEWS_PROCESSING_CHAIN = create_news_processing_chain()
    USE_LANGCHAIN = True
    print("✅ LangChain processing chain initialized successfully")
except Exception as e:
    USE_LANGCHAIN = False
    print(f"⚠️ LangChain initialization failed: {e}")
    print("⚠️ Falling back to direct Ollama calls")

def calculate_impact_assessment(sentiment, confidence, risk_score=None):
    """
    Calculate impact assessment label based on MarketAux API's sentiment and confidence scores.
    
    Args:
        sentiment: Float value from -1 to 1 (from MarketAux)
        confidence: Float value from 0 to 100 (from MarketAux)
        risk_score: Optional float value from 0 to 1
    
    Returns:
        String impact assessment label
    """
    try:
        # Convert to float to ensure proper comparison
        sentiment = float(sentiment) if sentiment is not None else 0
        confidence = float(confidence) if confidence is not None else 0
        risk_score = float(risk_score) if risk_score is not None else None
        
        # Log the inputs
        logger.info(f"Impact assessment inputs: sentiment={sentiment:.2f}, confidence={confidence:.1f}, risk_score={risk_score:.2f if risk_score is not None else 'None'}")
        
        # Check for VOLATILE / HIGH RISK condition first (overrides other conditions)
        if risk_score is not None and risk_score > 0.5:
            logger.info(f"Impact Assessment: VOLATILE (high risk_score: {risk_score:.2f})")
            return "VOLATILE"
            
        # Check for UNCERTAIN condition
        if confidence < 30:
            logger.info("Impact Assessment: UNCERTAIN (low confidence)")
            return "UNCERTAIN"
            
        # Check sentiment ranges with confidence thresholds
        if sentiment >= 0.5 and confidence >= 60:
            logger.info("Impact Assessment: STRONGLY POSITIVE")
            return "STRONGLY POSITIVE"
            
        elif 0.2 <= sentiment < 0.5 and confidence >= 40:
            logger.info("Impact Assessment: POSITIVE")
            return "POSITIVE"
            
        elif -0.2 < sentiment < 0.2:
            logger.info("Impact Assessment: NEUTRAL")
            return "NEUTRAL"
            
        elif -0.5 < sentiment <= -0.2 and confidence >= 40:
            logger.info("Impact Assessment: NEGATIVE")
            return "NEGATIVE"
            
        elif sentiment <= -0.5 and confidence >= 60:
            logger.info("Impact Assessment: STRONGLY NEGATIVE")
            return "STRONGLY NEGATIVE"
            
        else:
            # Fallback for cases not covered by the rules
            logger.info("Impact Assessment: NEUTRAL (fallback)")
            return "NEUTRAL"
            
    except Exception as e:
        logger.error(f"Error calculating impact assessment: {str(e)}")
        return "NEUTRAL"

def main():
    archive_path = 'news_archive.jsonl'
    
    # Indicate classification and processing methods
    if USE_DISTILBART:
        print(f"🤖 Using DistilBART for news classification (path: {DISTILBART_PATH})")
    else:
        print("📝 Using rule-based fallback for news classification (DistilBART not found)")
        
    if USE_LANGCHAIN:
        print("⚡ Using LangChain for sequential processing pipeline")
    else:
        print("🔄 Using direct Ollama calls for processing")
        
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
            
            with open(archive_path, 'a', encoding='utf-8') as f:
                for i, article in enumerate(articles, 1):
                    try:
                        publish_date = article.get('published_at')
                        if isinstance(publish_date, datetime):
                            publish_date = publish_date.isoformat()

                        # Extract entities from the API response
                        entities = article.get('entities', [])
                        sentiment = None
                        confidence = None
                        symbol = None
                        entity_name = None
                        
                        # Based on the API response format, entities contains symbol and name
                        if entities and isinstance(entities, list) and len(entities) > 0:
                            # Get the first entity data
                            first_entity = entities[0]
                            sentiment = first_entity.get('sentiment_score', None)
                            confidence = first_entity.get('match_score', None)
                            symbol = first_entity.get('symbol', None)
                            entity_name = first_entity.get('name', None)
                            
                            # Debug what we found in the entity
                            print(f"   🔍 Found entity: {symbol} - {entity_name}")

                        doc_id = get_doc_id(article)
                        if doc_id in existing_doc_ids:
                            print(f"\n⏭️  Skipping duplicate {i}/{len(articles)}: {article.get('title', '')[:50]}...")
                            skipped_articles += 1
                            continue

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
                            'symbol': symbol,
                            'entity_name': entity_name,
                            # 'entities': entities,
                        }

                        # NLP enrichment with detailed debugging
                        payload = enrich_news(payload, i, len(articles))

                        # Final debug output before saving
                        if symbol and entity_name:
                            print(f"   ✅ Including entity data - Symbol: {symbol}, Name: {entity_name}")
                        else:
                            print(f"   ⚠️ No entity symbol/name found in article")
                            
                        # Save and send
                        f.write(json.dumps(payload, ensure_ascii=False) + '\n')
                        producer.send(TOPIC, value=payload)
                        new_articles += 1
                        
                        print(f"   💾 Saved to archive & sent to Kafka")
                        
                    except Exception as article_error:
                        print(f"❌ Error processing article {i}: {article_error}")
                        continue

            producer.flush()
            
            total_time = time.time() - total_start_time
            print(f"\n📊 BATCH SUMMARY:")
            print(f"   📥 Total articles fetched: {len(articles)}")
            print(f"   ✅ New articles processed: {new_articles}")
            print(f"   ⏭️  Duplicate articles skipped: {skipped_articles}")
            print(f"   ⏱️  Total batch time: {total_time:.2f}s")
            print(f"   ⚡ Average time per article: {total_time/max(new_articles, 1):.2f}s")
            print(f"   💤 Sleeping for 5 minutes...")
            time.sleep(300)

        except Exception as e:
            print(f"❌ Batch Error: {e}")
            print(f"   🔄 Retrying in 60 seconds...")
            time.sleep(60)

if __name__ == '__main__':
    main()