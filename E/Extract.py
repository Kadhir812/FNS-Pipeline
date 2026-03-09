"""
Orchestrator for the financial news extraction pipeline.
This file coordinates modular services for API fetch, enrichment,
sentiment fusion, classification, deduplication, archive persistence,
and Kafka publishing.
"""

import time
from datetime import datetime

from services.config import ARCHIVE_PATH, DISTILBART_PATH, FINBERT_PATH, USE_DISTILBART, USE_FINBERT
from services.logging_setup import logger
from services.api_client import fetch_news
from services.kafka_service import create_producer, publish_article, flush
from services.archive_service import get_doc_id, load_existing_doc_ids, append_payloads_to_archive
from services.sentiment_service import (
    load_finbert_classifier,
    analyze_sentiment_with_finbert,
    compare_and_select_sentiment,
)
from services.classification_service import load_distilbart_classifier
from services.enrichment_service import enrich_news, clean_text, USE_LANGCHAIN


def main():
    producer = create_producer()

    print("\nEXTRACT.PY INITIALIZATION")
    print("=" * 50)

    if USE_DISTILBART:
        print("DistilBART ENABLED for news classification")
        print(f"Model path: {DISTILBART_PATH}")
        logger.info("INITIALIZATION: DistilBART classification ENABLED")
        load_distilbart_classifier()
    else:
        print("DistilBART NOT FOUND - Using keyword fallback only")
        logger.warning("INITIALIZATION: DistilBART classification DISABLED - keyword fallback only")

    if USE_FINBERT:
        print("FinBERT ENABLED for sentiment analysis")
        print(f"Model path: {FINBERT_PATH}")
        logger.info("INITIALIZATION: FinBERT sentiment analysis ENABLED")
        load_finbert_classifier()
    else:
        print("FinBERT NOT FOUND - Using MarketAux sentiment only")
        logger.warning("INITIALIZATION: FinBERT sentiment analysis DISABLED - MarketAux only")

    if USE_LANGCHAIN:
        print("LangChain ENABLED for sequential processing pipeline")
        logger.info("INITIALIZATION: LangChain processing pipeline ENABLED")
    else:
        print("LangChain NOT AVAILABLE - Using direct Ollama calls")
        logger.warning("INITIALIZATION: LangChain DISABLED - direct Ollama fallback")

    print("=" * 50)

    while True:
        try:
            print(f"\nStarting new batch at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            articles = fetch_news()
            print(f"Fetched {len(articles)} articles from API")

            existing_doc_ids = load_existing_doc_ids(ARCHIVE_PATH)
            print(f"Found {len(existing_doc_ids)} existing articles in archive")

            new_articles = 0
            skipped_articles = 0
            total_start_time = time.time()

            dedup_map = {}
            for index, article in enumerate(articles, 1):
                publish_date = article.get("published_at")
                if isinstance(publish_date, datetime):
                    publish_date = publish_date.isoformat()

                doc_id = get_doc_id(article)
                if doc_id in existing_doc_ids:
                    skipped_articles += 1
                    continue

                entities = article.get("entities", [])
                symbols = []
                entity_names = []
                sentiments = []
                confidences = []

                for entity in entities:
                    symbols.append(entity.get("symbol"))
                    entity_names.append(entity.get("name"))
                    sentiments.append(entity.get("sentiment_score"))
                    confidences.append(entity.get("match_score"))

                marketaux_sentiment = sentiments[0] if sentiments else None
                marketaux_confidence = confidences[0] if confidences else None

                article_text = clean_text(
                    article.get("snippet", "") or article.get("description", "") or article.get("title", "")
                )

                finbert_sentiment, finbert_confidence = analyze_sentiment_with_finbert(article_text)

                final_sentiment, final_confidence, sentiment_source = compare_and_select_sentiment(
                    marketaux_sentiment,
                    marketaux_confidence,
                    finbert_sentiment,
                    finbert_confidence,
                    index,
                )

                payload = {
                    "doc_id": doc_id,
                    "title": article.get("title", ""),
                    "description": article.get("description", ""),
                    "content": article.get("snippet", ""),
                    "sentiment": final_sentiment,
                    "confidence": final_confidence,
                    "sentiment_source": sentiment_source,
                    "publishedAt": publish_date,
                    "source": article.get("source", ""),
                    "link": article.get("url", ""),
                    "image_url": article.get("image_url", None),
                    "symbols": symbols,
                    "entity_names": entity_names,
                }

                payload = enrich_news(payload, index, len(articles))
                dedup_map[doc_id] = payload

            payloads = list(dedup_map.values())
            append_payloads_to_archive(ARCHIVE_PATH, payloads)

            for payload in payloads:
                publish_article(producer, payload)
                new_articles += 1

            flush(producer)

            total_time = time.time() - total_start_time
            total_minutes = total_time / 60

            print("\nBATCH SUMMARY:")
            print(f"Total articles fetched: {len(articles)}")
            print(f"New articles processed: {new_articles}")
            print(f"Duplicate articles skipped: {skipped_articles}")
            print(f"Total batch time: {total_minutes:.2f} minutes ({total_time:.2f} seconds)")

            time.sleep(600)

        except Exception as error:
            print(f"Batch Error: {error}")
            print("Retrying in 60 seconds...")
            time.sleep(60)


if __name__ == "__main__":
    main()
