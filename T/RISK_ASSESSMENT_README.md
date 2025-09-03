# Financial Risk Keywords and Assessment

This document explains how to use the enhanced risk keywords and risk assessment approach in the financial news sentiment analysis pipeline.

## Risk Keywords Files

There are two main risk keywords files available:

1. **Basic Risk Keywords** (`risk_keywords.txt`): The original risk keyword list with 40 terms.
2. **Enhanced Risk Keywords** (`enhanced_risk_keywords.txt`): A more comprehensive categorized list of financial risk terms, organized by categories such as Market Volatility, Financial Distress, Corporate Issues, etc.

## Using Risk Keywords in the Pipeline

### Approaches for Risk Assessment

1. **Basic Approach**: Simple keyword matching against a flat list of risk terms.
2. **Enhanced Approach**: Category-based risk assessment that:
   - Groups keywords by categories (Market Volatility, Financial Distress, etc.)
   - Applies category-specific weights to risk factors
   - Uses a more sophisticated scoring algorithm with diminishing returns

### Integration in the Pipeline

The risk assessment is now integrated into the `Extract.py` file and automatically:

1. Loads the enhanced risk keywords file if available (falls back to basic list)
2. Uses the `risk_utils.py` utility functions for advanced risk assessment if available
3. Provides detailed risk information including found keywords and categories

## How to Customize

### Adding Custom Keywords

To add custom risk keywords:

1. Edit the `enhanced_risk_keywords.txt` file
2. Add new terms under the appropriate category
3. Keep one term per line
4. Use `#` to mark category headers (e.g., `# Market Volatility`)

### Modifying Category Weights

To adjust how different types of risk factors influence the overall score:

1. Edit the `risk_utils.py` file
2. Modify the `category_weights` dictionary in the `calculate_risk_score` function:

```python
category_weights = {
    "market_volatility": 1.0,  # Default weight
    "financial_distress": 1.5,  # Higher weight for financial distress terms
    # Add other categories as needed
}
```

Higher weights mean those categories have more influence on the risk score.

## Risk Score Interpretation

The risk assessment produces several key values:

1. **risk_raw**: Base risk score between 0-1
2. **risk_adj**: Risk score adjusted by confidence
3. **risk_level**: Categorical risk level (low_risk, moderate_risk, high_risk)
4. **risk_details**: Detailed information about found keywords and categories

## Best Practices

1. **Regular Updates**: Financial markets evolve with new terminology. Regularly update the risk keywords list.
2. **Context Matters**: Consider the context in which risk terms appear. The current implementation checks for presence but not context.
3. **Balance**: Use a balance of general and specific terms for broader coverage.
4. **Industry-Specific**: Consider adding industry-specific risk terms for better accuracy in those domains.

## Technical Notes

The risk assessment implementation uses:
- Diminishing returns formula for keyword counting (prevents overemphasizing many weak signals)
- Confidence normalization using logarithmic scaling
- Hybrid approach that considers both textual risk factors and sentiment score
