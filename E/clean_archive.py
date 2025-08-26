import json

def remove_category_from_jsonl(input_file, output_file=None):
    """Remove category field from JSONL file"""
    if output_file is None:
        output_file = input_file
    
    cleaned_lines = []
    
    with open(input_file, 'r', encoding='utf-8') as f:
        for line_num, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
                
            try:
                # Parse JSON
                data = json.loads(line)
                
                # Remove category field if it exists
                if 'category' in data:
                    del data['category']
                
                # Convert back to JSON string
                cleaned_line = json.dumps(data, separators=(',', ':'))
                cleaned_lines.append(cleaned_line)
                
            except json.JSONDecodeError as e:
                print(f"❌ Error parsing line {line_num}: {e}")
                continue
    
    # Write cleaned data
    with open(output_file, 'w', encoding='utf-8') as f:
        for line in cleaned_lines:
            f.write(line + '\n')
    
    print(f"✅ Cleaned {len(cleaned_lines)} records")
    print(f"✅ Category field removed from {input_file}")

if __name__ == "__main__":
    remove_category_from_jsonl("news_archive.jsonl")