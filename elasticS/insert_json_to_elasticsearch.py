#!/usr/bin/env python3
"""
Elasticsearch JSON Data Insertion Tool
=====================================
This script inserts JSON/JSONL data into Elasticsearch with proper error handling,
batch processing, and progress tracking.

Usage:
    python insert_json_to_elasticsearch.py --file path/to/data.jsonl
    python insert_json_to_elasticsearch.py --file path/to/data.json --mode single
    python insert_json_to_elasticsearch.py --directory path/to/json_files/
"""

import json
import requests
import argparse
import os
import sys
from datetime import datetime
from typing import Dict, List, Any, Optional
import time

# Configuration
ES_URL = "http://localhost:9200"
INDEX_NAME = "news-analysis"
BATCH_SIZE = 100
REQUEST_TIMEOUT = 30

# Default data directories
E_DIR = "E"
MODEL_EVAL_DIR = "Model evaluation"

class ElasticsearchInserter:
    """Handle JSON data insertion into Elasticsearch"""
    
    def __init__(self, es_url: str = ES_URL, index_name: str = INDEX_NAME, batch_size: int = BATCH_SIZE):
        self.es_url = es_url
        self.index_name = index_name
        self.batch_size = batch_size
        self.session = requests.Session()
        
    def check_elasticsearch_connection(self) -> bool:
        """Check if Elasticsearch is accessible"""
        try:
            response = self.session.get(f"{self.es_url}/_cluster/health", timeout=5)
            if response.status_code == 200:
                health_data = response.json()
                print(f"✅ Connected to Elasticsearch - Status: {health_data.get('status', 'unknown')}")
                return True
            else:
                print(f"❌ Elasticsearch health check failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Cannot connect to Elasticsearch: {e}")
            return False
    
    def check_index_exists(self) -> bool:
        """Check if the target index exists"""
        try:
            response = self.session.get(f"{self.es_url}/{self.index_name}")
            exists = response.status_code == 200
            if exists:
                print(f"✅ Index '{self.index_name}' exists")
            else:
                print(f"⚠️ Index '{self.index_name}' does not exist")
            return exists
        except Exception as e:
            print(f"❌ Error checking index: {e}")
            return False
    
    def create_index_if_not_exists(self) -> bool:
        """Create index with proper mapping if it doesn't exist"""
        if self.check_index_exists():
            return True
        
        print(f"🔨 Creating index '{self.index_name}' with mapping...")
        
        mapping = {
            "mappings": {
                "properties": {
                    "doc_id": {"type": "keyword"},
                    "title": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 256}
                        }
                    },
                    "description": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 512}
                        }
                    },
                    "content": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword", "ignore_above": 1024}
                        }
                    },
                    "source": {"type": "keyword"},
                    "category": {"type": "keyword"},
                    "key_phrases": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "summary": {
                        "type": "text",
                        "fields": {
                            "keyword": {"type": "keyword"}
                        }
                    },
                    "impact_assessment": {"type": "keyword"},
                    "publishedAt": {"type": "date"},
                    "sentiment": {"type": "float"},
                    "confidence": {"type": "float"},
                    "risk_score": {"type": "float"},
                    "link": {"type": "keyword"},
                    "image_url": {"type": "keyword"}
                }
            }
        }
        
        try:
            response = self.session.put(
                f"{self.es_url}/{self.index_name}",
                headers={"Content-Type": "application/json"},
                data=json.dumps(mapping),
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                print(f"✅ Index '{self.index_name}' created successfully!")
                return True
            else:
                print(f"❌ Failed to create index: {response.text}")
                return False
                
        except Exception as e:
            print(f"❌ Error creating index: {e}")
            return False
    
    def validate_document(self, doc: Dict[str, Any]) -> Dict[str, Any]:
        """Validate and clean document before insertion"""
        cleaned_doc = {}
        
        # Required fields mapping
        field_mappings = {
            'doc_id': str,
            'title': str,
            'description': str,
            'content': str,
            'source': str,
            'category': str,
            'key_phrases': str,
            'summary': str,
            'impact_assessment': str,
            'publishedAt': str,
            'sentiment': float,
            'confidence': float,
            'risk_score': float,
            'link': str,
            'image_url': str
        }
        
        for field, expected_type in field_mappings.items():
            if field in doc:
                try:
                    if expected_type == float:
                        cleaned_doc[field] = float(doc[field]) if doc[field] is not None else 0.0
                    elif expected_type == str:
                        cleaned_doc[field] = str(doc[field]) if doc[field] is not None else ""
                    else:
                        cleaned_doc[field] = doc[field]
                except (ValueError, TypeError) as e:
                    print(f"⚠️ Warning: Invalid value for field '{field}': {doc.get(field)} - {e}")
                    if expected_type == float:
                        cleaned_doc[field] = 0.0
                    else:
                        cleaned_doc[field] = ""
        
        # Ensure doc_id exists
        if 'doc_id' not in cleaned_doc or not cleaned_doc['doc_id']:
            cleaned_doc['doc_id'] = f"auto_{int(time.time() * 1000000)}"
        
        return cleaned_doc
    
    def bulk_insert_documents(self, documents: List[Dict[str, Any]]) -> Dict[str, int]:
        """Insert documents using Elasticsearch bulk API"""
        if not documents:
            return {"success": 0, "failed": 0}
        
        # Prepare bulk request body
        bulk_body = []
        for doc in documents:
            validated_doc = self.validate_document(doc)
            
            # Add index action
            bulk_body.append(json.dumps({
                "index": {
                    "_index": self.index_name,
                    "_id": validated_doc.get("doc_id")
                }
            }))
            
            # Add document
            bulk_body.append(json.dumps(validated_doc))
        
        bulk_data = "\n".join(bulk_body) + "\n"
        
        try:
            response = self.session.post(
                f"{self.es_url}/_bulk",
                headers={"Content-Type": "application/x-ndjson"},
                data=bulk_data,
                timeout=REQUEST_TIMEOUT
            )
            
            if response.status_code == 200:
                result = response.json()
                success_count = 0
                failed_count = 0
                
                for item in result.get("items", []):
                    if "index" in item:
                        if item["index"].get("status") in [200, 201]:
                            success_count += 1
                        else:
                            failed_count += 1
                            error = item["index"].get("error", {})
                            doc_id = item["index"].get("_id", "unknown")
                            print(f"❌ Failed to insert document {doc_id}: {error}")
                
                return {"success": success_count, "failed": failed_count}
            else:
                print(f"❌ Bulk insert failed: {response.status_code} - {response.text}")
                return {"success": 0, "failed": len(documents)}
                
        except Exception as e:
            print(f"❌ Error during bulk insert: {e}")
            return {"success": 0, "failed": len(documents)}
    
    def insert_from_jsonl_file(self, file_path: str) -> Dict[str, int]:
        """Insert data from a JSONL file"""
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return {"success": 0, "failed": 0}
        
        print(f"📖 Reading JSONL file: {file_path}")
        
        total_success = 0
        total_failed = 0
        line_number = 0
        batch = []
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                for line in file:
                    line_number += 1
                    line = line.strip()
                    
                    if not line:
                        continue
                    
                    try:
                        doc = json.loads(line)
                        batch.append(doc)
                        
                        # Process batch when it reaches batch_size
                        if len(batch) >= self.batch_size:
                            print(f"📦 Processing batch of {len(batch)} documents (lines {line_number - len(batch) + 1}-{line_number})...")
                            result = self.bulk_insert_documents(batch)
                            total_success += result["success"]
                            total_failed += result["failed"]
                            print(f"   ✅ Success: {result['success']}, ❌ Failed: {result['failed']}")
                            batch = []
                    
                    except json.JSONDecodeError as e:
                        print(f"⚠️ Invalid JSON on line {line_number}: {e}")
                        total_failed += 1
                
                # Process remaining documents in the last batch
                if batch:
                    print(f"📦 Processing final batch of {len(batch)} documents...")
                    result = self.bulk_insert_documents(batch)
                    total_success += result["success"]
                    total_failed += result["failed"]
                    print(f"   ✅ Success: {result['success']}, ❌ Failed: {result['failed']}")
        
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            return {"success": total_success, "failed": total_failed + 1}
        
        return {"success": total_success, "failed": total_failed}
    
    def insert_from_json_file(self, file_path: str) -> Dict[str, int]:
        """Insert data from a single JSON file (array of objects)"""
        if not os.path.exists(file_path):
            print(f"❌ File not found: {file_path}")
            return {"success": 0, "failed": 0}
        
        print(f"📖 Reading JSON file: {file_path}")
        
        try:
            with open(file_path, 'r', encoding='utf-8') as file:
                data = json.load(file)
            
            # Handle both single object and array of objects
            if isinstance(data, list):
                documents = data
            elif isinstance(data, dict):
                documents = [data]
            else:
                print("❌ Invalid JSON format: expected object or array of objects")
                return {"success": 0, "failed": 1}
            
            total_success = 0
            total_failed = 0
            
            # Process in batches
            for i in range(0, len(documents), self.batch_size):
                batch = documents[i:i + self.batch_size]
                print(f"📦 Processing batch {i // self.batch_size + 1} of {len(batch)} documents...")
                
                result = self.bulk_insert_documents(batch)
                total_success += result["success"]
                total_failed += result["failed"]
                print(f"   ✅ Success: {result['success']}, ❌ Failed: {result['failed']}")
            
            return {"success": total_success, "failed": total_failed}
            
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON file: {e}")
            return {"success": 0, "failed": 1}
        except Exception as e:
            print(f"❌ Error reading file: {e}")
            return {"success": 0, "failed": 1}
    
    def insert_from_directory(self, directory_path: str) -> Dict[str, int]:
        """Insert data from all JSON/JSONL files in a directory"""
        if not os.path.exists(directory_path):
            print(f"❌ Directory not found: {directory_path}")
            return {"success": 0, "failed": 0}
        
        print(f"📂 Processing directory: {directory_path}")
        
        total_success = 0
        total_failed = 0
        processed_files = 0
        
        # Find all JSON and JSONL files
        for filename in os.listdir(directory_path):
            file_path = os.path.join(directory_path, filename)
            
            if not os.path.isfile(file_path):
                continue
            
            if filename.lower().endswith('.jsonl'):
                print(f"\n📄 Processing JSONL file: {filename}")
                result = self.insert_from_jsonl_file(file_path)
                total_success += result["success"]
                total_failed += result["failed"]
                processed_files += 1
                
            elif filename.lower().endswith('.json'):
                print(f"\n📄 Processing JSON file: {filename}")
                result = self.insert_from_json_file(file_path)
                total_success += result["success"]
                total_failed += result["failed"]
                processed_files += 1
        
        if processed_files == 0:
            print("⚠️ No JSON or JSONL files found in the directory")
        
        return {"success": total_success, "failed": total_failed}
    
    def get_index_stats(self) -> Optional[Dict[str, Any]]:
        """Get statistics about the index"""
        try:
            response = self.session.get(f"{self.es_url}/{self.index_name}/_stats")
            if response.status_code == 200:
                return response.json()
            return None
        except Exception as e:
            print(f"❌ Error getting index stats: {e}")
            return None

def get_default_data_file():
    """Find the first available default data file"""
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    default_paths = [
        os.path.join(script_dir, "..", E_DIR, "news_archive.jsonl"),
        os.path.join(script_dir, "..", MODEL_EVAL_DIR, "news_archive.jsonl"),
        os.path.join(script_dir, "..", MODEL_EVAL_DIR, "news_enriched_deepseek-r1_7b.jsonl"),
        os.path.join(script_dir, "..", MODEL_EVAL_DIR, "news_enriched_martain7r_finance-llama-8b_q4_k_m.jsonl")
    ]
    
    for path in default_paths:
        if os.path.exists(path):
            return path
    
    return None

def main():
    parser = argparse.ArgumentParser(description="Insert JSON/JSONL data into Elasticsearch")
    parser.add_argument("--file", "-f", help="Path to JSON or JSONL file")
    parser.add_argument("--directory", "-d", help="Path to directory containing JSON/JSONL files")
    parser.add_argument("--mode", "-m", choices=["jsonl", "json"], default="jsonl", 
                       help="File format mode (default: jsonl)")
    parser.add_argument("--index", "-i", default=INDEX_NAME, help=f"Elasticsearch index name (default: {INDEX_NAME})")
    parser.add_argument("--url", "-u", default=ES_URL, help=f"Elasticsearch URL (default: {ES_URL})")
    parser.add_argument("--batch-size", "-b", type=int, default=BATCH_SIZE, 
                       help=f"Batch size for bulk operations (default: {BATCH_SIZE})")
    
    args = parser.parse_args()
    
    # If no arguments provided, use default paths
    if not args.file and not args.directory:
        default_file = get_default_data_file()
        
        if default_file:
            print(f"📝 No file specified, using default: {default_file}")
            args.file = default_file
            args.mode = "jsonl"
        else:
            print("❌ Error: No default files found and no --file or --directory specified")
            print("Please ensure one of these files exists:")
            script_dir = os.path.dirname(os.path.abspath(__file__))
            default_paths = [
                os.path.join(script_dir, "..", E_DIR, "news_archive.jsonl"),
                os.path.join(script_dir, "..", MODEL_EVAL_DIR, "news_archive.jsonl")
            ]
            for path in default_paths:
                print(f"  📄 {path}")
            parser.print_help()
            sys.exit(1)
    
    # Initialize inserter
    inserter = ElasticsearchInserter(
        es_url=args.url,
        index_name=args.index,
        batch_size=args.batch_size
    )
    
    print("🚀 Elasticsearch JSON Data Insertion Tool")
    print("=" * 50)
    print(f"Target Index: {args.index}")
    print(f"Elasticsearch URL: {args.url}")
    print(f"Batch Size: {args.batch_size}")
    print()
    
    # Check connection and create index if needed
    if not inserter.check_elasticsearch_connection():
        sys.exit(1)
    
    if not inserter.create_index_if_not_exists():
        sys.exit(1)
    
    # Start insertion
    start_time = time.time()
    
    if args.file:
        print(f"📝 Processing single file: {args.file}")
        if args.mode == "jsonl":
            result = inserter.insert_from_jsonl_file(args.file)
        else:
            result = inserter.insert_from_json_file(args.file)
    else:
        print(f"📁 Processing directory: {args.directory}")
        result = inserter.insert_from_directory(args.directory)
    
    # Summary
    end_time = time.time()
    duration = end_time - start_time
    
    print("\n" + "=" * 50)
    print("📊 INSERTION SUMMARY")
    print("=" * 50)
    print(f"✅ Successfully inserted: {result['success']} documents")
    print(f"❌ Failed insertions: {result['failed']} documents")
    print(f"⏱️ Total time: {duration:.2f} seconds")
    
    if result['success'] > 0:
        print(f"📈 Average speed: {result['success'] / duration:.2f} docs/second")
    
    # Show index stats
    stats = inserter.get_index_stats()
    if stats:
        doc_count = stats.get("indices", {}).get(args.index, {}).get("total", {}).get("docs", {}).get("count", 0)
        index_size = stats.get("indices", {}).get(args.index, {}).get("total", {}).get("store", {}).get("size_in_bytes", 0)
        print(f"📋 Total documents in index: {doc_count}")
        print(f"💾 Index size: {index_size / 1024 / 1024:.2f} MB")
    
    print("\n🎉 Operation completed!")

if __name__ == "__main__":
    main()
