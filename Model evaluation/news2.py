import json
import requests
import re
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError

OLLAMA_URL = "http://localhost:11434/api/generate"
OLLAMA_MODEL = "deepseek-r1:7b"  # Change to test other models

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'Financenews-raw'

# SKIP ALREADY PROCESSED ARTICLES
SKIP_COUNT = 3  # Skip first 3 articles, start from 4th

# Create producer with error handling
def create_kafka_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            api_version=(0, 10, 1),
            retries=3,
            max_block_ms=5000
        )
        print("✅ Kafka producer connected successfully!")
        return producer
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        print("⚠️ Continuing without Kafka ingestion...")
        return None

producer = create_kafka_producer()

def clean_text(text):
    if text:
        return re.sub(r'<[^>]+>', '', text)
    return ''

def extract_final_answer(text):
    """Remove reasoning blocks and extract clean answer"""
    if isinstance(text, str):
        # Remove <think>...</think> blocks
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        # Remove extra whitespace and quotes
        text = text.strip().strip('"').strip("'")
        # Extract after "Output:" if present
        if "Output:" in text:
            text = text.split("Output:")[-1].strip()
        # Remove any remaining explanation text
        lines = text.split('\n')
        # Take the last non-empty line (usually the clean answer)
        for line in reversed(lines):
            line = line.strip()
            if line and not line.startswith('Explanation') and not line.startswith('The'):
                return line
    return text

def ollama_query(prompt, text):
    response = requests.post(
        OLLAMA_URL,
        json={"model": OLLAMA_MODEL, "prompt": prompt.replace('+ text', text)},
        stream=True
    )
    full_reply = ""
    for line in response.iter_lines(decode_unicode=True):
        if line:
            try:
                data = json.loads(line)
                full_reply += data.get("response", "")
            except json.JSONDecodeError as e:
                print(f"❌ Ollama JSON decode error: {e}")
                print(f"Line: {line}")
    return extract_final_answer(full_reply.strip())

# Simplified prompts with same meaning
key_phrases_prompt = (
    "Extract key financial phrases from this article that affect stock prices. "
    "Return comma-separated phrases, 1-5 words each, lowercase. "
    "Include tickers, numbers, company names. No explanations.\n\n+ text\n\nKey phrases:"
)

category_prompt = (
    "Classify this article into ONE category: "
    "A. STOCK-RATING-CHANGE, B. EARNINGS, C. M&A, D. REGULATION, E. GENERAL-MARKET. "
    "Return only the letter and category name.\n\n+ text\n\nCategory:"
)

summary_prompt = (
    "Write a 15-word summary of this financial news. "
    "Include: company + action + impact. Be concise.\n\n+ text\n\nSummary:"
)

impact_prompt = (
    "Rate this news impact on stock price: POSITIVE, NEGATIVE, or NEUTRAL. "
    "Return only one word.\n\n+ text\n\nImpact:"
)

def enrich_ollama_fields(article, article_num, total_articles):
    print(f"\n📰 Processing Article {article_num}/{total_articles}")
    print(f"Title: {article.get('title', 'No title')[:60]}...")
    
    # Copy unchanged fields
    enriched = {
        "doc_id": article.get("doc_id"),
        "title": article.get("title"),
        "description": article.get("description"),
        "content": article.get("content"),
        "publishedAt": article.get("publishedAt"),
        "source": article.get("source"),
        "sentiment": article.get("sentiment"),
        "confidence": article.get("confidence"),
        "risk_score": article.get("risk_score"),
        "link": article.get("link"),
        "image_url": article.get("image_url"),
    }
    
    # Use content or description for enrichment
    text = clean_text(article.get('content', '') or article.get('description', ''))
    
    print("  🔤 Generating key phrases...")
    enriched["key_phrases"] = ollama_query(key_phrases_prompt, text)
    print("  ✅ Key phrases done")
    
    print("  📂 Generating category...")
    enriched["category"] = ollama_query(category_prompt, text)
    print("  ✅ Category done")
    
    print("  📝 Generating summary...")
    enriched["summary"] = ollama_query(summary_prompt, text)
    print("  ✅ Summary done")
    
    print("  📊 Generating impact assessment...")
    enriched["impact_assessment"] = ollama_query(impact_prompt, text)
    print("  ✅ Impact assessment done")
    
    print(f"🎉 Article {article_num}/{total_articles} FINISHED!")
    return enriched

input_path = "d:\\big data pipeline\\Model evaluation\\news_archive.jsonl"
output_path = f"d:\\big data pipeline\\Model evaluation\\news_enriched_{OLLAMA_MODEL.replace(':','_').replace('/','_')}.jsonl"

BATCH_SIZE = 4

def batch_iterable(iterable, batch_size):
    batch = []
    for item in iterable:
        batch.append(item)
        if len(batch) == batch_size:
            yield batch
            batch = []
    if batch:
        yield batch

with open(input_path, "r", encoding="utf-8") as fin:
    lines = list(fin)
    total_articles = len(lines)
    
    # Skip already processed articles
    if SKIP_COUNT > 0:
        print(f"⏭️ Skipping first {SKIP_COUNT} articles (already processed)")
        lines = lines[SKIP_COUNT:]
        remaining_articles = len(lines)
        print(f"📊 Processing remaining {remaining_articles} articles out of {total_articles} total")
    else:
        remaining_articles = total_articles
    
    # Open output file in append mode to continue from where we left off
    mode = "a" if SKIP_COUNT > 0 else "w"
    with open(output_path, mode, encoding="utf-8") as fout:
        article_counter = SKIP_COUNT  # Start counter from skip count
        
        print(f"🚀 Starting enrichment from article {SKIP_COUNT + 1} with {OLLAMA_MODEL}")
        
        for batch_num, batch in enumerate(batch_iterable(lines, BATCH_SIZE), 1):
            print(f"\n🔄 Processing Batch {batch_num} ({len(batch)} articles)")
            enriched_batch = []
            
            for line in batch:
                article_counter += 1
                article = json.loads(line)
                enriched = enrich_ollama_fields(article, article_counter, total_articles)
                enriched_batch.append(enriched)
                fout.write(json.dumps(enriched, ensure_ascii=False) + "\n")
            
            # Kafka ingestion for the batch (only if producer is available)
            if producer:
                print(f"\n📤 Sending Batch {batch_num} to Kafka...")
                for enriched_article in enriched_batch:
                    try:
                        producer.send(TOPIC, enriched_article)
                        print(f"Sent to Kafka: {enriched_article.get('doc_id')}")
                    except Exception as e:
                        print(f"❌ Failed to send to Kafka: {e}")
                producer.flush()
            else:
                print(f"\n⚠️ Skipping Kafka ingestion for Batch {batch_num} (Kafka not available)")
            
            print(f"✅ Batch {batch_num} complete!")
            # Sleep disabled - processing will continue immediately

print(f"\n🎊 ALL DONE! Enriched file written to {output_path}")
if producer:
    print("✅ All enriched articles ingested into Kafka.")
else:
    print("⚠️ Kafka ingestion was skipped. Start Kafka and run dummy.py to ingest later.")