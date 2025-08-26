import os
from transformers import pipeline

MODELS_DIR = os.path.join(os.path.dirname(__file__), "models")
DISTILBART_PATH = os.path.join(MODELS_DIR, "distilbart-mnli")

# Load model
classifier = pipeline(
    "zero-shot-classification",
    model=DISTILBART_PATH,
    device=-1,
    local_files_only=True
)

# 10 Complete Categories
categories = [
    "earnings and quarterly results",
    "analyst ratings and recommendations", 
    "mergers acquisitions and deals",
    "regulatory and legal developments",
    "economic data and federal reserve",
    "corporate actions and leadership",
    "market trends and sector analysis",
    "ipo and new listings",
    "product launches and innovation",
    "general business and industry news"
]

category_map = {
    "earnings and quarterly results": ("A", "EARNINGS"),
    "analyst ratings and recommendations": ("B", "ANALYST-RATINGS"), 
    "mergers acquisitions and deals": ("C", "M&A"),
    "regulatory and legal developments": ("D", "REGULATORY"),
    "economic data and federal reserve": ("E", "ECONOMIC-DATA"),
    "corporate actions and leadership": ("F", "CORPORATE-ACTIONS"),
    "market trends and sector analysis": ("G", "MARKET-TRENDS"),
    "ipo and new listings": ("H", "IPO-LISTINGS"),
    "product launches and innovation": ("I", "PRODUCT-NEWS"),
    "general business and industry news": ("J", "GENERAL-BUSINESS")
}

def classify_news(text):
    result = classifier(text, categories)
    predicted = result['labels'][0]
    confidence = result['scores'][0]
    
    letter, name = category_map.get(predicted, ("J", "GENERAL-BUSINESS"))
    return f"Category: {letter} ({name}) - Confidence: {confidence:.3f}"

# Test cases covering all 10 categories
test_cases = [
    "Apple reports record Q4 earnings with $89.5B revenue beating estimates",                    # A
    "Goldman Sachs upgrades Tesla to buy with $350 price target",                               # B  
    "Microsoft acquires Activision Blizzard for $68.7 billion in gaming deal",                 # C
    "SEC charges cryptocurrency exchange with securities violations",                           # D
    "Federal Reserve raises interest rates 0.75% to combat inflation",                         # E
    "Apple announces 4-for-1 stock split and increased dividend",                              # F
    "Technology sector leads market rally as AI stocks surge",                                 # G
    "Rivian prices IPO at $78 per share in largest offering of 2021",                         # H
    "Apple unveils iPhone 15 with breakthrough camera technology",                             # I
    "General Motors reports strong vehicle sales in European markets"                          # J
]

print("🎯 Testing 10-Category Financial News Classification\n")

for i, text in enumerate(test_cases, 1):
    result = classify_news(text)
    print(f"{i:2d}. {text}")
    print(f"    {result}\n")