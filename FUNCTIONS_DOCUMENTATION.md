# Extract.py and Transform.py Functions Documentation

## Extract.py Functions

### Utility Functions

#### `safe_format(value, format_spec=".2f")`
**Use Case:** Safely formats numeric values that might be None, preventing errors during string formatting operations. Returns "N/A" if value is None or cannot be formatted.

**Location:** E/Extract.py

---

#### `clean_text(text)`
**Use Case:** Removes all XML/HTML tags from text content using regex pattern matching. Essential for cleaning scraped article content before processing.

**Location:** E/Extract.py

---

#### `get_doc_id(article)`
**Use Case:** Generates a unique SHA256 hash ID from article title and snippet for deduplication. Ensures each unique article is processed only once.

**Location:** E/Extract.py

---

#### `load_existing_doc_ids(path)`
**Use Case:** Loads previously processed article IDs from the archive file (news_archive.jsonl) to avoid reprocessing duplicates across pipeline restarts.

**Location:** E/Extract.py

---

### API & Data Fetching

#### `fetch_news()`
**Use Case:** Fetches financial news articles from MarketAux API with filters:
- Category: Finance
- Language: English
- Country: US

Returns a list of raw article data from the API.

**Location:** E/Extract.py

---

### AI/ML Processing

#### `ollama_query(prompt, text)`
**Use Case:** Sends prompts to Ollama LLM API (DeepSeek-R1:1.5b) for text generation tasks including:
- Generating article summaries
- Extracting key phrases
- NLP enrichment

Handles API requests and returns generated text or None on failure.

**Location:** E/Extract.py

---

#### `clean_ollama_response(response)`
**Use Case:** Cleans DeepSeek-R1 model responses by:
- Removing `<think>...</think>` reasoning blocks
- Stripping XML tags
- Cleaning whitespace and newlines
- Extracting meaningful content from verbose responses

Returns clean, usable text output.

**Location:** E/Extract.py

---

#### `get_ollama_model()`
**Use Case:** Initializes and returns an Ollama LLM model instance configured for LangChain integration. Used for creating processing pipelines.

**Location:** E/Extract.py

---

#### `create_news_processing_chain()`
**Use Case:** Creates a LangChain pipeline that processes articles in parallel:
- **Branch 1:** Extracts key phrases from article content
- **Branch 2:** Generates concise article summaries

Uses LangChain's `RunnablePassthrough` for parallel execution and `StrOutputParser` for output handling.

**Location:** E/Extract.py

---

### Classification Functions

#### `classify_with_distilbart(text)`
**Use Case:** Uses DistilBART zero-shot classification model to categorize news into 14 financial categories (A-N):
- **A:** Stock Market / General Market News
- **B:** Economic Data / GDP / Inflation
- **C:** Central Bank / Monetary Policy
- **D:** Corporate Earnings / Financial Results
- **E:** Mergers & Acquisitions
- **F:** Insider Trading / Executive Actions
- **G:** IPO / New Listings
- **H:** Dividends / Shareholder Returns
- **I:** Commodity Markets
- **J:** General Business News
- **K:** Technology / Innovation
- **L:** Regulatory / Legal / Compliance
- **M:** Real Estate / Housing
- **N:** International Trade / Tariffs

Returns category letter and confidence score.

**Location:** E/Extract.py

---

#### `keyword_based_category(text)`
**Use Case:** Fallback classification method using Redis-stored keywords. Matches article text against category-specific keyword sets loaded from Redis. Returns the first matching category or "J" (General Business) as default.

**Note:** This function appears twice in the code.

**Location:** E/Extract.py

---

#### `calculate_keyword_scores(text)`
**Use Case:** Calculates weighted scores for all 14 categories based on:
- Keyword frequency in text
- Keyword specificity (inverse document frequency concept)
- Normalized scoring (0-1 range)

Returns dictionary mapping each category to its normalized score.

**Location:** E/Extract.py

---

#### `check_rule_based_overrides(text)`
**Use Case:** Checks for high-confidence rule-based category overrides that should take precedence over ML classification:
- **Insider trading** terms → Category F
- **IPO filing** terms → Category G
- **Merger/acquisition** terms → Category E
- **SEC filing** terms → Category L
- **Fed meeting** terms → Category C

Returns category letter if override applies, otherwise None.

**Location:** E/Extract.py

---

#### `weighted_category_classification(text, article_num)`
**Use Case:** Combined weighted scoring approach:
- **70% weight:** DistilBART ML classification
- **30% weight:** Redis keyword-based scoring

Combines both methods for more accurate classification. Returns:
- Final category
- Final confidence score
- Method used
- Score breakdown for analysis

**Location:** E/Extract.py

---

#### `enhanced_classify_news_category(text, payload, article_num)`
**Use Case:** Main classification orchestrator that:
1. Checks for rule-based overrides first
2. Uses weighted scoring (DistilBART + keywords)
3. Falls back to pure keyword method if needed
4. Logs classification decisions and confidence

Returns enriched payload with category and confidence fields.

**Location:** E/Extract.py

---

### Article Enrichment

#### `enrich_news(payload, article_num, total_articles)`
**Use Case:** Main enrichment function that adds multiple fields to articles:
1. **Category classification** - Uses enhanced classification
2. **Key phrases extraction** - Via LangChain pipeline
3. **Summary generation** - Via LangChain pipeline
4. **Sentiment override** - Based on positive/negative keywords in summary

Coordinates the entire enrichment process using the LangChain pipeline or falls back to individual processing.

**Location:** E/Extract.py

---

#### `fallback_to_individual_processing(payload, text, article_num)`
**Use Case:** Fallback enrichment method when LangChain pipeline fails:
- Makes direct Ollama API calls for key phrases
- Makes direct Ollama API calls for summary
- Processes sequentially instead of parallel

Ensures pipeline continues even if LangChain encounters errors.

**Location:** E/Extract.py

---

### Main Execution

#### `main()`
**Use Case:** Main execution loop that orchestrates the entire Extract pipeline:
1. Loads existing article IDs from archive
2. Fetches news from MarketAux API
3. Deduplicates articles by doc_id
4. Aggregates tickers for identical articles
5. Enriches each article with NLP (category, summary, key phrases)
6. Saves to news_archive.jsonl
7. Sends to Kafka topic (Financenews-raw)
8. Waits 10 minutes and repeats

Includes colored console output, progress tracking, and error handling.

**Location:** E/Extract.py

---

## Transform.py Functions

### Batch Processing Function

#### `write_to_es(batch_df, batch_id)`
**Use Case:** Core transformation function that processes each Kafka streaming batch:

**Step 1: Data Cleaning**
- Fills null values with defaults to prevent processing errors
- Ensures all required fields have valid values

**Step 2: Confidence Normalization**
- Uses logarithmic scaling: `log(1 + confidence) / log(1 + max_conf)`
- Prevents saturation and maintains resolution across confidence range
- Normalizes to 0-1 range for consistent weighting

**Step 3: Context-Aware Risk Level Calculation**
- Matches keywords for risk assessment:
  - **High Risk:** Negative sentiment + risk keywords OR commodity news + negative sentiment
  - **Medium Risk:** M&A/strategic news + positive sentiment
  - **Moderate Risk:** Negative sentiment without specific context
  - **Low Risk:** Default for positive/neutral articles

**Step 4: Risk Score Conversion**
- Maps risk levels to numeric base scores:
  - High Risk: 0.9
  - Moderate Risk: 0.7
  - Medium Risk: 0.5
  - Low Risk: 0.1

**Step 5: Hybrid Risk Score Adjustment**
- Formula: `(risk_base * 0.8) + (risk_base * conf_norm * 0.2)`
- Balances base risk with confidence-weighted adjustment
- Stores both raw and adjusted scores for analysis

**Step 6: Unique Document ID Generation**
- Creates SHA256 hash from: symbol + source + title + publishedAt
- Prevents duplicate entries in Elasticsearch
- Ensures data integrity across ingestion

**Step 7: Impact Assessment (Two-Step Process)**
- **Step 7a: Sentiment-Based Classification**
  - Sentiment > 0.5 → BULLISH
  - Sentiment 0.1 to 0.5 → POSITIVE
  - Sentiment -0.1 to 0.1 → NEUTRAL
  - Sentiment -0.5 to -0.1 → NEGATIVE
  - Sentiment < -0.5 → BEARISH

- **Step 7b: Risk Override**
  - High risk + negative sentiment → HIGH RISK (overrides bearish)
  - Ensures critical risks are properly flagged

**Step 8: Impact Score Mapping**
- BULLISH: 2.0
- POSITIVE: 1.0
- NEUTRAL: 0.0
- NEGATIVE: -1.0
- BEARISH: -2.0
- HIGH RISK: -3.0

**Step 9: Final Score Calculation**
- Formula: `(impact_score * conf_norm) - (risk_raw * (1 - conf_norm))`
- **Strong signals (high confidence):** Impact score dominates
- **Weak signals (low confidence):** Risk score dominates
- Prevents unfair penalization of high-confidence positive news
- Adds conservatism for low-confidence signals

**Step 10: Additional Metrics**
- Calculates `sentiment_confidence_score` for analysis
- Timestamp parsing with multiple format fallbacks
- UTC timezone normalization

**Step 11: Elasticsearch Connection Test**
- Tests connection before writing batch
- Handles connection failures gracefully

**Step 12: Data Writing**
- Writes enriched data to Elasticsearch index `news-analysis`
- Uses upsert operation (update or insert)
- Implements retry logic (3 attempts)
- Includes debug logging and error handling

**Returns:** None (writes directly to Elasticsearch)

**Location:** T/Transform.py

---

## Non-Function Logic in Transform.py

While Transform.py has only one function, it contains extensive inline PySpark processing:

### Configuration & Setup
- **Checkpoint Management:** Manages Kafka offset tracking for fault tolerance
- **Spark Session:** Initializes with Elasticsearch and Kafka connectors, JAR dependencies
- **Schema Definition:** Defines 17-field schema including arrays for symbols and entities

### Data Ingestion
- **Kafka Stream Reading:** Connects to `Financenews-raw` topic with earliest offset
- **JSON Parsing:** Applies schema to incoming messages
- **Array Processing:** Extracts first element from symbols and entity_names arrays

### Risk Keyword Loading
- **File Reading:** Loads keywords from `T/enhanced_risk_keywords.txt`
- **Condition Building:** Creates dynamic PySpark conditions for keyword matching

### Stream Processing
- **DataFrame Transformations:** Multiple chained transformations using PySpark SQL
- **Context Detection:** M&A keywords, commodity keywords, risk keywords
- **Sentiment Analysis:** Impact assessment with risk overlays

### Elasticsearch Management
- **Index Checking:** HTTP HEAD request to verify index existence
- **Auto-Creation:** Creates index with proper mappings if missing
- **Field Mapping:** 21 fields including dates (epoch_millis), keywords, text fields

### Stream Execution
- **Streaming Query:** Uses `writeStream` with `foreachBatch` pattern
- **Checkpoint-Based State:** Ensures exactly-once processing semantics
- **Continuous Processing:** Runs indefinitely with `awaitTermination()`

---

## Pipeline Flow Summary

### Extract Pipeline (Extract.py)
```
MarketAux API → Fetch Articles → Deduplicate → Classify Category → 
Generate Summary → Extract Key Phrases → Override Sentiment → 
Save to Archive → Send to Kafka (Financenews-raw)
```

### Transform Pipeline (Transform.py)
```
Kafka (Financenews-raw) → Parse JSON → Normalize Confidence → 
Calculate Risk → Assess Impact → Calculate Final Score → 
Generate Doc ID → Write to Elasticsearch (news-analysis)
```

---

**Generated:** January 4, 2026  
**Version:** 1.0  
**Files Documented:** E/Extract.py, T/Transform.py
