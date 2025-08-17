import json
import re
from kafka import KafkaProducer

KAFKA_BROKER = 'localhost:29092'
TOPIC = 'Financenews-raw'
input_path = "d:\\big data pipeline\\E\\news_archive.jsonl"

def extract_final_answer(text):
    if isinstance(text, str):
        text = re.sub(r'<think>.*?</think>', '', text, flags=re.DOTALL)
        text = text.strip().strip('"')
        if "Output:" in text:
            text = text.split("Output:")[-1].strip()
    return text

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open(input_path, "r", encoding="utf-8") as fin:
    for line in fin:
        article = json.loads(line)
        # Preprocess ollama fields if present
        for field in ["key_phrases", "category", "summary", "impact_assessment"]:
            if field in article:
                article[field] = extract_final_answer(article[field])
        producer.send(TOPIC, article)
        print(f"Sent to Kafka: {article.get('doc_id')}")

producer.flush()
print("✅ All enriched articles ingested into Kafka.")