import requests
import json
import sys
from datetime import datetime

# Configuration
ES_HOST = "localhost"
ES_PORT = 9200
ES_INDEX = "news-analysis"  # Your Elasticsearch index name

def delete_all_articles():
    """Delete all articles from the Elasticsearch index."""
    print(f"⚠️ WARNING: This will delete ALL articles from the '{ES_INDEX}' index!")
    
    confirmation = input("Type 'DELETE ALL' to confirm: ")
    if confirmation != "DELETE ALL":
        print("Deletion cancelled.")
        return
    
    # First, get count of documents to delete
    count_url = f"http://{ES_HOST}:{ES_PORT}/{ES_INDEX}/_count"
    try:
        response = requests.get(count_url)
        if response.status_code == 404:
            print(f"Index '{ES_INDEX}' does not exist.")
            return
        
        count_data = response.json()
        doc_count = count_data.get("count", 0)
        print(f"Found {doc_count} documents to delete.")
    
        if doc_count == 0:
            print("No documents to delete.")
            return
            
        # Delete by query - removes all documents
        delete_url = f"http://{ES_HOST}:{ES_PORT}/{ES_INDEX}/_delete_by_query"
        delete_query = {
            "query": {
                "match_all": {}
            }
        }
        
        print(f"Deleting all documents from '{ES_INDEX}'...")
        delete_response = requests.post(delete_url, json=delete_query)
        
        if delete_response.status_code in [200, 201]:
            result = delete_response.json()
            print(f"✅ Successfully deleted {result.get('deleted', 0)} documents")
            if result.get('failures', []):
                print(f"⚠️ Had {len(result.get('failures'))} failures during deletion")
        else:
            print(f"❌ Failed to delete documents: {delete_response.status_code}")
            print(delete_response.text)
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")

if __name__ == "__main__":
    delete_all_articles()