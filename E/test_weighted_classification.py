#!/usr/bin/env python3
"""
Test script to verify weighted classification integration
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

# Test the weighted classification functions
def test_weighted_classification():
    print("🧪 Testing weighted classification integration...")
    
    # Sample financial news text
    test_text = """
    Apple Inc. reported strong quarterly earnings that beat analyst expectations. 
    The company's revenue surged 15% year-over-year, driven by robust iPhone sales 
    and growing services revenue. Investors reacted positively to the news, 
    sending Apple shares up 8% in after-hours trading.
    """
    
    # Test payload
    test_payload = {
        'title': 'Apple Reports Strong Q4 Earnings',
        'summary': 'Apple beats earnings expectations with strong iPhone sales',
        'content': test_text,
        'sentiment': 0.8
    }
    
    try:
        # This would normally require the full Extract.py environment
        # but we can at least verify the functions exist
        from Extract import (
            enhanced_classify_news_category,
            weighted_category_classification,
            calculate_keyword_scores,
            check_rule_based_overrides
        )
        print("✅ All weighted classification functions imported successfully!")
        
        # Test individual components
        print("🔍 Testing rule-based overrides...")
        ipo_text = "Company XYZ files for initial public offering with SEC"
        rule_result = check_rule_based_overrides(ipo_text)
        print(f"   IPO text → Rule override: {rule_result}")
        
        insider_text = "CEO sold insider trading shares worth $10 million"
        rule_result = check_rule_based_overrides(insider_text)
        print(f"   Insider trading text → Rule override: {rule_result}")
        
        print("✅ Weighted classification system is properly integrated!")
        
    except ImportError as e:
        print(f"❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"❌ Test error: {e}")
        return False
    
    return True

if __name__ == "__main__":
    test_weighted_classification()
