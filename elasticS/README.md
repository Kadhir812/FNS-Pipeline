# Elasticsearch JSON Data Insertion Tool

This directory contains tools for inserting JSON and JSONL data into Elasticsearch.

## Files

- `insert_json_to_elasticsearch.py` - Main insertion tool
- `update_elastic_mapping.py` - Tool for updating Elasticsearch mappings
- `example_usage.py` - Usage examples and demonstrations
- `requirements.txt` - Python dependencies

## Quick Start

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Ensure Elasticsearch is Running

Make sure Elasticsearch is running on `http://localhost:9200` (default).

### 3. Insert Data

**From a single JSONL file:**
```bash
python insert_json_to_elasticsearch.py --file "../E/news_archive.jsonl"
```

**From a single JSON file:**
```bash
python insert_json_to_elasticsearch.py --file "data.json" --mode json
```

**From all JSON/JSONL files in a directory:**
```bash
python insert_json_to_elasticsearch.py --directory "../Model evaluation"
```

## Features

### 🚀 **Batch Processing**
- Processes documents in configurable batches (default: 100)
- Optimized for large datasets

### 🔍 **Data Validation**
- Validates document structure before insertion
- Handles missing or invalid fields gracefully
- Auto-generates doc_id if missing

### 📊 **Progress Tracking**
- Real-time progress updates
- Detailed success/failure statistics
- Performance metrics

### 🛡️ **Error Handling**
- Robust error handling for network issues
- Detailed error messages for failed insertions
- Continues processing despite individual failures

### 🔧 **Flexible Configuration**
- Custom Elasticsearch URL
- Custom index name
- Adjustable batch size
- Support for both JSON and JSONL formats

## Command Line Options

```
python insert_json_to_elasticsearch.py [OPTIONS]

Options:
  --file, -f FILE          Path to JSON or JSONL file
  --directory, -d DIR      Path to directory containing JSON/JSONL files
  --mode, -m MODE          File format mode: 'jsonl' or 'json' (default: jsonl)
  --index, -i INDEX        Elasticsearch index name (default: news-analysis)
  --url, -u URL            Elasticsearch URL (default: http://localhost:9200)
  --batch-size, -b SIZE    Batch size for bulk operations (default: 100)
```

## Data Format

The tool expects documents with the following structure (all fields optional):

```json
{
  "doc_id": "unique_document_id",
  "title": "Article title",
  "description": "Article description",
  "content": "Full article content",
  "source": "news_source.com",
  "category": "news_category",
  "key_phrases": "extracted, key, phrases",
  "summary": "Article summary",
  "impact_assessment": "HIGH|MEDIUM|LOW|NEUTRAL",
  "publishedAt": "2025-08-17T07:21:55.000000Z",
  "sentiment": 0.75,
  "confidence": 95.5,
  "risk_score": 0.3,
  "link": "https://example.com/article",
  "image_url": "https://example.com/image.jpg"
}
```

## Index Mapping

The tool automatically creates an index with the proper mapping if it doesn't exist. The mapping includes:

- **Text fields** with keyword sub-fields for exact matching
- **Date fields** for temporal queries
- **Float fields** for numerical analysis
- **Keyword fields** for categorical data

## Examples

### Insert from JSONL file with custom settings:
```bash
python insert_json_to_elasticsearch.py \
  --file "../E/news_archive.jsonl" \
  --index "my-custom-index" \
  --batch-size 50 \
  --url "http://localhost:9200"
```

### Process all files in Model evaluation directory:
```bash
python insert_json_to_elasticsearch.py --directory "../Model evaluation"
```

### Insert single JSON object:
```bash
python insert_json_to_elasticsearch.py --file "single_article.json" --mode json
```

## Troubleshooting

### Connection Issues
- Ensure Elasticsearch is running: `curl http://localhost:9200`
- Check firewall settings
- Verify Elasticsearch URL is correct

### Performance Issues
- Reduce batch size for memory-constrained environments
- Increase batch size for better performance on powerful systems
- Monitor Elasticsearch cluster health

### Data Issues
- Check JSON format validity
- Ensure file encoding is UTF-8
- Verify file permissions

## Integration with Existing Tools

This tool works alongside:
- `update_elastic_mapping.py` - for updating index mappings
- The broader data pipeline in the workspace
- Elasticsearch monitoring and analytics tools

## Available Data Files

Your workspace contains these data files ready for insertion:
- `../E/news_archive.jsonl` - Extracted news data
- `../Model evaluation/news_archive.jsonl` - Model evaluation data
- `../Model evaluation/news_enriched_*.jsonl` - AI-enriched news data
