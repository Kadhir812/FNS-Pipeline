# Corrected Financial News Sentiment Processing Algorithm

## Algorithm 1: Financial News Sentiment Processing Pipeline

**Input:** Continuous stream of financial news articles from MarketAux API  
**Output:** Enriched financial news records containing sentiment score, risk score, and impact score stored in Elasticsearch and visualized on dashboards.

---

### 1. Initialize System Components
```
Start Apache Kafka streaming service (port 29092)
Start Apache Spark processing engine (Structured Streaming)
Initialize Redis keyword scoring database (14 categories)
Initialize Elasticsearch storage system (index: financial_news)
Load DistilBART model for category classification
Load FinBERT model for sentiment analysis
```

### 2. Data Extraction
```
Connect to MarketAux financial news API
Fetch latest news articles Ai with metadata
Extract entity-level sentiment scores from API
```

### 3. Data Preprocessing
```
For each article Ai:
    Extract title, description, snippet, timestamp, source, entities
    Remove HTML tags: clean_text(content)
    Extract symbols and entity names from API response
    Normalize text for NLP processing
```

### 4. Duplicate Detection
```
Generate unique hash Hi = SHA256(title + snippet)
Load existing document IDs from archive
If Hi already exists in archive:
    Skip article (increment skipped_count)
Else:
    Continue processing
```

### 5. Hybrid Sentiment Analysis (NEW)
```
Get MarketAux sentiment Sm and confidence Cm from API
Analyze with FinBERT: Sf, Cf = FinBERT(text)

If both Sm and Sf available:
    If agree (same direction) AND |Sm - Sf| < 0.3:
        # Confidence-weighted average
        Final_Sentiment = (Sm × Cm + Sf × Cf) / (Cm + Cf)
        Final_Confidence = min(Cm + Cf, 1.0)
        Source = "HYBRID_AGREEMENT"
    Else:
        # Select higher confidence
        If Cf > Cm:
            Final_Sentiment = Sf
            Final_Confidence = Cf
            Source = "FINBERT_DOMINANT"
        Else:
            Final_Sentiment = Sm
            Final_Confidence = Cm
            Source = "MARKETAUX_DOMINANT"
Else if only Sm available:
    Final_Sentiment = Sm
    Final_Confidence = Cm
    Source = "MARKETAUX_ONLY"
Else if only Sf available (Fallback):
    Final_Sentiment = Sf
    Final_Confidence = Cf
    Source = "FINBERT_FALLBACK"
```

### 6. Hybrid Category Classification
```
Classify with DistilBART:
    categories = [earnings, M&A, regulatory, ...]
    Sm = DistilBART_ZeroShot(text, categories)

Score with Redis keywords:
    For each category c:
        Sk[c] = keyword_match_score(text, redis_keywords[c])

# Weighted combination
ALPHA = 0.7  # ML weight
BETA = 0.3   # Keywords weight
For each category c:
    Combined_Score[c] = ALPHA × Sm[c] + BETA × Sk[c]

Final_Category = argmax(Combined_Score)
If Combined_Score[Final_Category] < 0.4:
    Check rule-based overrides
    If no override: Default to "J" (general business)
```

### 7. NLP Enrichment
```
If LangChain available:
    results = SEQUENTIAL_CHAIN.invoke(text)
    key_phrases = results['key_phrases']
    summary = results['summary']
Else (Fallback):
    key_phrases = direct_ollama_call(phrases_prompt, text)
    summary = direct_ollama_call(summary_prompt, text)

Apply sentiment override:
    If positive_keywords in summary: sentiment = max(sentiment, 0.7)
    If negative_keywords in summary: sentiment = min(sentiment, -0.7)
```

### 8. Data Streaming
```
Construct payload = {
    doc_id, title, description, content,
    sentiment, confidence, sentiment_source,
    category, key_phrases, summary,
    symbols[], entity_names[],
    publishedAt, source, link, image_url
}

Publish to Kafka topic "Financenews-raw"
Save to local archive: news_archive.jsonl
```

### 9. Stream Processing with Spark
```
df = Spark.readStream
    .format("kafka")
    .option("subscribe", "Financenews-raw")
    .load()

Parse JSON and extract fields
Normalize confidence:
    conf_norm = log(1 + confidence) / log(1 + max_conf)
```

### 10. Risk Assessment
```
Load enhanced risk keywords from file
Match keywords in title and content

Determine risk level:
    If risk_keywords AND sentiment < 0: risk_level = "high_risk"
    If M&A_keywords AND sentiment > 0.2: risk_level = "medium_risk"
    If commodity_keywords AND sentiment < 0: risk_level = "high_risk"
    If sentiment < 0: risk_level = "moderate_risk"
    Else: risk_level = "low_risk"

Map to numeric:
    risk_base = {
        "high_risk": 0.9,
        "moderate_risk": 0.7,
        "medium_risk": 0.5,
        "low_risk": 0.1
    }

Calculate adjusted risk:
    risk_score = (risk_base × 0.8) + (risk_base × conf_norm × 0.2)
```

### 11. Impact Assessment (Multi-Stage)
```
Stage 1: Sentiment-based impact
    impact_assessment = {
        "BULLISH" if sentiment >= 0.7,
        "POSITIVE" if sentiment >= 0.4,
        "SLIGHTLY POSITIVE" if sentiment >= 0.1,
        "NEUTRAL" if -0.1 < sentiment < 0.1,
        "SLIGHTLY NEGATIVE" if sentiment <= -0.1,
        "NEGATIVE" if sentiment <= -0.4,
        "BEARISH" if sentiment <= -0.7
    }

Stage 2: Risk override
    If risk_base >= 0.8:
        impact_assessment = "HIGH RISK"
    If 0.5 <= risk_base < 0.8:
        impact_assessment = "MODERATE RISK"

Stage 3: Numeric mapping
    impact_score = {
        "BULLISH": 2.0,
        "POSITIVE": 1.0,
        "SLIGHTLY POSITIVE": 0.5,
        "NEUTRAL": 0.0,
        "SLIGHTLY NEGATIVE": -0.5,
        "NEGATIVE": -1.0,
        "BEARISH": -2.0,
        "HIGH RISK": -3.0,
        "MODERATE RISK": -1.5
    }
```

### 12. Final Score Calculation
```
# Confidence-Weighted Risk Formula
final_score = (impact_score × conf_norm) - (risk_base × (1 - conf_norm))

# Sentiment-confidence product
sentiment_confidence_score = sentiment × conf_norm
```

### 13. Data Storage
```
enriched_record = {
    title, description, content, publishedAt, source,
    sentiment, confidence, conf_norm,
    risk_level, risk_raw, risk_adj,
    key_phrases, category, summary,
    impact_assessment, impact_score,
    final_score, sentiment_confidence_score,
    doc_id, symbol, entity_name,
    symbols[], entity_names[]
}

Write to Elasticsearch index "financial_news"
Use batch writes with error handling
```

### 14. API Access Layer
```
Node.js Express server exposes REST APIs:
    GET /api/v1/articles - Search and filter articles
    GET /api/v1/articles/:id - Retrieve single article
    GET /api/v1/metrics/sentiment - Sentiment distribution
    GET /api/v1/metrics/categories - Category breakdown
    GET /api/v1/health - System health status

Implement Redis caching for frequent queries
Apply rate limiting for API protection
```

### 15. Visualization Dashboard
```
React frontend displays:
    ✓ Real-time article feed with filters
    ✓ Sentiment trend charts
    ✓ Category distribution pie charts
    ✓ Risk level indicators
    ✓ Company-specific analytics
    ✓ Source breakdown
    ✓ Impact assessment visualizations
    
Support dark/light themes
Enable sorting by date, sentiment, risk, or final score
```

---

## Key Mathematical Formulas

### Category Classification
```
Category_Score[c] = 0.7 × DistilBART[c] + 0.3 × Redis_Keywords[c]
Final_Category = argmax(Category_Score)
```

### Sentiment Analysis
```
Hybrid_Sentiment = {
    (Sm × Cm + Sf × Cf) / (Cm + Cf),  if agree
    max_confidence_source,             if disagree
}
```

### Confidence Normalization
```
conf_norm = log(1 + confidence) / log(1 + 1000)
```

### Risk Score
```
risk_score = (risk_base × 0.8) + (risk_base × conf_norm × 0.2)
```

### Final Score
```
final_score = (impact_score × conf_norm) - (risk_base × (1 - conf_norm))
```

---

## System Architecture Summary

```
MarketAux API → Extract.py (Python)
                    ↓ [preprocessing, sentiment, category]
                Kafka Topic: Financenews-raw
                    ↓ [streaming]
                Transform.py (PySpark)
                    ↓ [risk, impact, final score]
                Elasticsearch
                    ↓ [REST API]
                Node.js Backend (Express)
                    ↓ [HTTP/JSON]
                React Frontend (Dashboard)
```

---

## Technical Implementation Details

**Languages & Frameworks:**
- Python 3.x (data extraction & preprocessing)
- Apache Spark 3.3.0 (stream processing)
- Node.js + Express (backend API)
- React + Vite (frontend)

**NLP Models:**
- DistilBART-MNLI (zero-shot category classification)
- FinBERT (financial sentiment analysis)
- Ollama DeepSeek-R1 1.5B (text generation)

**Data Infrastructure:**
- Apache Kafka 3.0.0 (message broker)
- Elasticsearch 8.10.2 (search & analytics)
- Redis 7.x (keyword scoring & caching)

**Processing Characteristics:**
- Batch interval: 10 minutes
- Average latency: 30-60 seconds per article
- Throughput: ~100 articles per batch
- CPU-based inference (device=-1)
