import requests
from kafka import KafkaProducer
import json
import time
from datetime import datetime
import re
import hashlib

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'Financenews-raw'
MARKETAUX_API_KEY =   # <-- Add your API key here

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

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
    print(r.text)  # Debug the actual response content
    try:
        data = r.json()
    except ValueError:
        print("❌ Not a valid JSON response!")
        print(r.text)  # Check for HTML/XML or error message
        return []
    return data.get("data", [])

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "deepseek-r1:7b"

def ollama_query(prompt, text):
    try:
        response = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt.replace('+ text', text)},
            stream=True,
            timeout=30
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

def enrich_news(payload, article_num, total_articles):
    text = clean_text(payload.get('content', '') or payload.get('description', ''))
    
    if not text:
        print(f"   ⚠️  No text content available for processing")
        text = payload.get('title', '')
    
    print(f"\n🔄 Processing Article {article_num}/{total_articles}")
    print(f"   📰 Title: {payload.get('title', 'N/A')[:80]}...")
    print(f"   📝 Text Length: {len(text)} chars")
    
    start_time = time.time()

    # KEY PHRASES - Advanced NER with financial focus
    print(f"   🔍 Extracting key phrases...")
    try:
        key_phrases_prompt = (
            "Extract financial entities: tickers, amounts, percentages, companies, dates, actions.\n"
            "Format: lowercase, comma-separated, max 15 items.\n"
            "Text: " + text + "\n"
            "Entities:"
        )
        payload['key_phrases'] = ollama_query(key_phrases_prompt, text)
        print(f"   ✅ Key phrases: {payload['key_phrases'][:50]}...")
    except Exception as e:
        print(f"   ❌ Key phrases error: {e}")
        payload['key_phrases'] = "N/A"

    # CATEGORY - Binary decision tree approach
    print(f"   📂 Classifying category...")
    try:
        category_prompt = (
            "Classify news type:\n"
            "A=STOCK-RATING-CHANGE B=EARNINGS C=M&A D=REGULATION E=GENERAL-MARKET\n"
            "Return single letter only.\n"
            "Text: " + text + "\n"
            "Class:"
        )
        raw_category = ollama_query(category_prompt, text)
        # Additional validation for category
        category_match = re.search(r'\b([ABCDE])\b', raw_category)
        payload['category'] = category_match.group(1) if category_match else "E"
        print(f"   ✅ Category: {payload['category']}")
    except Exception as e:
        print(f"   ❌ Category error: {e}")
        payload['category'] = "E"

    # SUMMARY - Constraint-based generation
    print(f"   📋 Generating summary...")
    try:
        summary_prompt = (
            "One sentence summary: Subject + Action + Impact (<15 words).\n"
            "Text: " + text + "\n"
            "Summary:"
        )
        payload['summary'] = ollama_query(summary_prompt, text)
        print(f"   ✅ Summary: {payload['summary'][:60]}...")
    except Exception as e:
        print(f"   ❌ Summary error: {e}")
        payload['summary'] = "Financial news update"

    # IMPACT - Sentiment with financial context
    print(f"   📊 Assessing impact...")
    try:
        impact_prompt = (
            "Market impact: POSITIVE/NEGATIVE/NEUTRAL\n"
            "Consider: earnings, ratings, M&A, regulations, guidance.\n"
            "Text: " + text + "\n"
            "Impact:"
        )
        raw_impact = ollama_query(impact_prompt, text)
        # Additional validation for impact
        if "POSITIVE" in raw_impact.upper():
            payload['impact_assessment'] = "POSITIVE"
        elif "NEGATIVE" in raw_impact.upper():
            payload['impact_assessment'] = "NEGATIVE"
        else:
            payload['impact_assessment'] = "NEUTRAL"
        print(f"   ✅ Impact: {payload['impact_assessment']}")
    except Exception as e:
        print(f"   ❌ Impact error: {e}")
        payload['impact_assessment'] = "NEUTRAL"

    processing_time = time.time() - start_time
    print(f"   ⏱️  Processing time: {processing_time:.2f}s")
    print(f"   🎉 Article {article_num}/{total_articles} COMPLETED!")
    
    return payload

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

def main():
    archive_path = 'news_archive.jsonl'
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

                        entities = article.get('entities', [])
                        sentiment = None
                        confidence = None
                        if entities and isinstance(entities, list):
                            first_entity = entities[0]
                            sentiment = first_entity.get('sentiment_score', None)
                            confidence = first_entity.get('match_score', None)

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
                            # 'entities': entities,
                        }

                        # NLP enrichment with detailed debugging
                        payload = enrich_news(payload, i, len(articles))

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