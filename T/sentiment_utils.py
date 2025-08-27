def map_financial_sentiment(score, confidence, text, risk_score=None):
    """
    Map sentiment score and confidence to a finance-oriented impact label.
    All logic is self-contained to avoid serialization issues.
    """
    # Handle None values
    if score is None:
        score = 0.0
    if confidence is None:
        confidence = 0.0
    if text is None:
        text = ""
    if risk_score is None:
        risk_score = 0.0
        
    try:
        score = float(score)
        confidence = float(confidence)
        risk_score = float(risk_score)
    except (ValueError, TypeError):
        return "Uncertain"

    # Normalize confidence if it's in 0-100 range
    if confidence > 1.0:
        confidence = confidence / 100.0
        
    # Check for VOLATILE / HIGH RISK condition first (overrides other conditions)
    if risk_score > 0.5:
        return "VOLATILE"
        
    # Define high risk keywords directly in the function
    high_risk_keywords = ["bankruptcy", "lawsuit", "scandal", "fraud", "investigation", 
                         "default", "crash", "collapse", "crisis", "urgent", "emergency", 
                         "plunge", "disaster"]
    
    # Risk keywords detection (VOLATILE condition)
    text_lower = str(text).lower() if text else ""
    if any(word in text_lower for word in high_risk_keywords):
        return "VOLATILE"

    # Speculative language detection
    speculative_keywords = ["may", "might", "could", "expected", "likely", "rumor", "speculation"]
    if any(word in text_lower for word in speculative_keywords):
        return "Speculative"

    # Confidence check
    if confidence < 0.3:
        return "Uncertain"

    # Score-based mapping
    if score >= 0.6:
        return "Bullish"
    elif score >= 0.2:
        return "Slightly Bullish"
    elif score > -0.2:
        return "Neutral"
    elif score > -0.6:
        return "Slightly Bearish"
    else:
        return "Bearish"