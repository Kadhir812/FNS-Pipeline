# Save as: update-elasticsearch-mapping.py
import requests
import json

ES_URL = "http://localhost:9200"
INDEX_NAME = "news-analysis"

def check_index_exists():
    """Check if the index exists"""
    response = requests.get(f"{ES_URL}/{INDEX_NAME}")
    return response.status_code == 200

def get_current_mapping():
    """Get current mapping of the index"""
    try:
        response = requests.get(f"{ES_URL}/{INDEX_NAME}/_mapping")
        if response.status_code == 200:
            return response.json()
        return None
    except Exception as e:
        print(f"❌ Error getting current mapping: {e}")
        return None

def compare_mappings(current_mapping, desired_mapping):
    """Compare current mapping with desired mapping"""
    try:
        if not current_mapping:
            return False, "No current mapping found"
        
        current_props = current_mapping.get(INDEX_NAME, {}).get('mappings', {}).get('properties', {})
        desired_props = desired_mapping['mappings']['properties']
        
        differences = []
        
        # Check for missing fields or different field types
        for field, desired_config in desired_props.items():
            if field not in current_props:
                differences.append(f"Missing field: {field}")
            else:
                current_config = current_props[field]
                # Check if field types match
                if current_config.get('type') != desired_config.get('type'):
                    differences.append(f"Type mismatch for {field}: current={current_config.get('type')}, desired={desired_config.get('type')}")
                
                # Check for missing multi-fields
                current_fields = current_config.get('fields', {})
                desired_fields = desired_config.get('fields', {})
                
                for subfield, subfield_config in desired_fields.items():
                    if subfield not in current_fields:
                        differences.append(f"Missing subfield: {field}.{subfield}")
        
        return len(differences) == 0, differences
    except Exception as e:
        return False, [f"Error comparing mappings: {e}"]

def get_desired_mapping():
    """Define the desired mapping structure"""
    return {
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

def backup_data():
    """Backup existing data to a temporary index"""
    print("📦 Creating backup of existing data...")
    
    # Create backup index
    backup_index = f"{INDEX_NAME}_backup_{int(__import__('time').time())}"
    
    reindex_body = {
        "source": {"index": INDEX_NAME},
        "dest": {"index": backup_index}
    }
    
    response = requests.post(f"{ES_URL}/_reindex",
                           headers={"Content-Type": "application/json"},
                           data=json.dumps(reindex_body))
    
    if response.status_code == 200:
        print(f"✅ Data backed up to: {backup_index}")
        return backup_index
    else:
        print(f"❌ Backup failed: {response.text}")
        return None

def restore_data(backup_index):
    """Restore data from backup index"""
    print(f"🔄 Restoring data from backup: {backup_index}")
    
    reindex_body = {
        "source": {"index": backup_index},
        "dest": {"index": INDEX_NAME}
    }
    
    response = requests.post(f"{ES_URL}/_reindex",
                           headers={"Content-Type": "application/json"},
                           data=json.dumps(reindex_body))
    
    if response.status_code == 200:
        print("✅ Data restored successfully!")
        
        # Clean up backup index
        cleanup_response = requests.delete(f"{ES_URL}/{backup_index}")
        if cleanup_response.status_code == 200:
            print(f"🗑️ Backup index {backup_index} cleaned up")
        
        return True
    else:
        print(f"❌ Restore failed: {response.text}")
        return False

def delete_index():
    """Delete the existing index"""
    print(f"🗑️ Deleting existing index: {INDEX_NAME}")
    response = requests.delete(f"{ES_URL}/{INDEX_NAME}")
    
    if response.status_code == 200:
        print("✅ Index deleted successfully!")
        return True
    else:
        print(f"❌ Failed to delete index: {response.text}")
        return False

def create_index_with_mapping(mapping):
    """Create index with the specified mapping"""
    print(f"🔨 Creating index with updated mapping: {INDEX_NAME}")
    
    response = requests.put(f"{ES_URL}/{INDEX_NAME}",
                          headers={"Content-Type": "application/json"},
                          data=json.dumps(mapping))
    
    if response.status_code == 200:
        print("✅ Index created with new mapping!")
        return True
    else:
        print(f"❌ Failed to create index: {response.text}")
        return False

def update_mapping_with_reindex():
    """Complete mapping update process with data preservation"""
    print("\n🚀 Starting mapping update process...")
    
    # Step 1: Check if index exists
    if not check_index_exists():
        print("📝 Index doesn't exist, creating new one...")
        desired_mapping = get_desired_mapping()
        return create_index_with_mapping(desired_mapping)
    
    # Step 2: Get current mapping
    print("🔍 Checking current mapping...")
    current_mapping = get_current_mapping()
    desired_mapping = get_desired_mapping()
    
    # Step 3: Compare mappings
    is_compatible, differences = compare_mappings(current_mapping, desired_mapping)
    
    if is_compatible:
        print("✅ Current mapping is already up to date!")
        return True
    
    print("⚠️ Mapping differences found:")
    for diff in differences:
        print(f"   - {diff}")
    
    # Step 4: Check if index has data
    count_response = requests.get(f"{ES_URL}/{INDEX_NAME}/_count")
    doc_count = 0
    if count_response.status_code == 200:
        doc_count = count_response.json().get('count', 0)
    
    print(f"📊 Index contains {doc_count} documents")
    
    if doc_count == 0:
        print("📝 Index is empty, safe to recreate...")
        if delete_index() and create_index_with_mapping(desired_mapping):
            print("✅ Empty index recreated with new mapping!")
            return True
        return False
    
    # Step 5: Reindex process for non-empty index
    print("🔄 Index contains data, performing reindex process...")
    
    # Backup data
    backup_index = backup_data()
    if not backup_index:
        print("❌ Failed to backup data, aborting...")
        return False
    
    try:
        # Delete and recreate index
        if not delete_index():
            print("❌ Failed to delete index, aborting...")
            return False
        
        if not create_index_with_mapping(desired_mapping):
            print("❌ Failed to create new index, aborting...")
            return False
        
        # Restore data
        if not restore_data(backup_index):
            print("❌ Failed to restore data!")
            return False
        
        print("🎉 Mapping update completed successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error during reindex process: {e}")
        print(f"⚠️ Your data is safe in backup index: {backup_index}")
        return False

def verify_mapping():
    """Verify the final mapping is correct"""
    print("\n🔍 Verifying final mapping...")
    
    response = requests.get(f"{ES_URL}/{INDEX_NAME}/_mapping")
    if response.status_code == 200:
        mapping = response.json()
        properties = mapping.get(INDEX_NAME, {}).get('mappings', {}).get('properties', {})
        
        print("✅ Final mapping verification:")
        for field, config in properties.items():
            field_type = config.get('type', 'unknown')
            subfields = list(config.get('fields', {}).keys())
            subfields_str = f" (subfields: {', '.join(subfields)})" if subfields else ""
            print(f"   - {field}: {field_type}{subfields_str}")
        
        return True
    else:
        print("❌ Failed to verify mapping")
        return False

if __name__ == "__main__":
    print("🔧 Elasticsearch Mapping Update Tool")
    print("=" * 50)
    
    try:
        # Check Elasticsearch connection
        health_response = requests.get(f"{ES_URL}/_cluster/health")
        if health_response.status_code != 200:
            print("❌ Cannot connect to Elasticsearch. Make sure it's running on localhost:9200")
            exit(1)
        
        print("✅ Connected to Elasticsearch")
        
        # Perform mapping update
        success = update_mapping_with_reindex()
        
        if success:
            verify_mapping()
            print("\n🎉 All operations completed successfully!")
        else:
            print("\n❌ Mapping update failed!")
            
    except Exception as e:
        print(f"❌ Unexpected error: {e}")