# Transform.py Calculations Guide

## 📊 Complete Guide to All Calculations

This document explains every calculation performed in Transform.py in simple, easy-to-understand terms with examples.

---

## Table of Contents
1. [Confidence Normalization (conf_norm)](#1-confidence-normalization-conf_norm)
2. [Risk Level Classification (risk_level)](#2-risk-level-classification-risk_level)
3. [Risk Base Score (risk_raw)](#3-risk-base-score-risk_raw)
4. [Adjusted Risk Score (risk_adj)](#4-adjusted-risk-score-risk_adj)
5. [Impact Assessment (impact_assessment)](#5-impact-assessment-impact_assessment)
6. [Impact Score (impact_score)](#6-impact-score-impact_score)
7. [Final Composite Score (final_score)](#7-final-composite-score-final_score)
8. [Sentiment Confidence Score](#8-sentiment-confidence-score)
9. [Complete Example Flow](#9-complete-example-flow)

---

## 1. Confidence Normalization (conf_norm)

### Purpose
Convert raw confidence scores (0-1000+) to a normalized 0-1 scale using logarithmic scaling.

### Formula
```
conf_norm = log(1 + confidence) / log(1 + 1000)
```

### Why Logarithmic?
- **Prevents saturation**: The difference between confidence 900 vs 950 matters less than 50 vs 100
- **Maintains resolution**: Small differences at low confidence are critical
- **Creates smooth range**: Produces clean 0-1 values for weighting

### Examples
| Raw Confidence | Calculation | conf_norm |
|---------------|-------------|-----------|
| 50 | log(51) / log(1001) | **0.567** |
| 500 | log(501) / log(1001) | **0.902** |
| 900 | log(901) / log(1001) | **0.976** |

### When It's Used
- Weighting risk scores
- Calculating final_score
- Determining sentiment_confidence_score

---

## 2. Risk Level Classification (risk_level)

### Purpose
Categorize articles into risk levels based on keyword matching and sentiment analysis.

### Output Values
- `"high_risk"` - Very dangerous situation
- `"moderate_risk"` - Concerning situation
- `"medium_risk"` - Cautious situation (often M&A related)
- `"low_risk"` - Minimal concern

### Decision Logic (Evaluated in Order)

#### Priority 1: High Risk Keywords + Negative Sentiment
```
IF (contains risk keywords) AND (sentiment < 0)
→ "high_risk"
```
**Keywords**: bankruptcy, default, crash, loss, investigation, lawsuit, fraud, violation, etc.

**Example**: "Company files for bankruptcy protection" → **high_risk**

#### Priority 2: M&A Keywords + Positive Sentiment
```
IF (contains M&A keywords) AND (sentiment > 0.2)
→ "medium_risk"
```
**Keywords**: m&a, merger, acquisition, deal, strategic, expansion, growth

**Example**: "Tesla announces $5B acquisition deal" → **medium_risk**

#### Priority 3: Commodity Keywords + Negative Sentiment
```
IF (contains commodity keywords) AND (sentiment < 0)
→ "high_risk"
```
**Keywords**: output, capacity, margin, commodity, smelter, cut, price, supply, demand

**Example**: "Copper smelter cuts output by 50%" → **high_risk**

#### Priority 4: Any Negative Sentiment
```
IF (sentiment < 0)
→ "moderate_risk"
```
**Example**: "Stocks decline on weak economic outlook" → **moderate_risk**

#### Default: Low Risk
```
ELSE → "low_risk"
```
**Example**: "Company reports strong quarterly earnings" → **low_risk**

---

## 3. Risk Base Score (risk_raw)

### Purpose
Convert categorical risk_level into numeric value for mathematical calculations.

### Mapping Table
| risk_level | risk_raw | Interpretation |
|------------|----------|----------------|
| `"high_risk"` | **0.9** | 90% risk weight - very dangerous |
| `"moderate_risk"` | **0.7** | 70% risk weight - concerning |
| `"medium_risk"` | **0.5** | 50% risk weight - cautious |
| `"low_risk"` | **0.1** | 10% risk weight - minimal concern |

### Example
```
risk_level = "high_risk"
→ risk_raw = 0.9
```

### Usage
- Base for calculating risk_adj
- Used in final_score penalty term
- Determines impact_assessment overrides

---

## 4. Adjusted Risk Score (risk_adj)

### Purpose
Blend base risk (80%) with confidence-adjusted risk (20%) to create final risk score.

### Formula
```
risk_adj = (risk_raw × 0.8) + (risk_raw × conf_norm × 0.2)
```

### Why Hybrid Approach?
- **80% base risk**: Core risk assessment stays stable and reliable
- **20% confidence boost**: High confidence slightly increases risk impact
- **Prevents over-reliance**: Confidence alone doesn't dominate the score

### Calculation Breakdown
```
risk_adj = (risk_raw × 0.8)  +  (risk_raw × conf_norm × 0.2)
           ─────────────────     ──────────────────────────────
           Stable base (80%)     Confidence adjustment (20%)
```

### Example
**Input**:
- risk_raw = 0.9
- conf_norm = 0.567

**Calculation**:
```
risk_adj = (0.9 × 0.8) + (0.9 × 0.567 × 0.2)
risk_adj = 0.72 + 0.102
risk_adj = 0.822
```

**Interpretation**: High risk (0.9) with moderate confidence slightly reduces to 0.822.

### Comparison Table
| risk_raw | conf_norm | risk_adj | Effect |
|----------|-----------|----------|--------|
| 0.9 | 0.2 | 0.756 | Low confidence → lower adjusted risk |
| 0.9 | 0.5 | 0.810 | Medium confidence → moderate adjustment |
| 0.9 | 0.9 | 0.882 | High confidence → higher adjusted risk |

---

## 5. Impact Assessment (impact_assessment)

### Purpose
Create human-readable impact label combining sentiment analysis and risk evaluation.

### Two-Step Process

#### STEP A: Sentiment-Based Assessment

```
IF confidence < 20        → "NEUTRAL" (too uncertain)
ELSE IF sentiment ≥ 0.7   → "BULLISH" (very positive)
ELSE IF sentiment ≥ 0.4   → "POSITIVE" (moderately positive)
ELSE IF sentiment ≥ 0.1   → "SLIGHTLY POSITIVE" (mildly positive)
ELSE IF sentiment ≤ -0.7  → "BEARISH" (very negative)
ELSE IF sentiment ≤ -0.4  → "NEGATIVE" (moderately negative)
ELSE IF sentiment ≤ -0.1  → "SLIGHTLY NEGATIVE" (mildly negative)
ELSE                      → "NEUTRAL" (near zero sentiment)
```

#### STEP B: Risk Override (Takes Precedence)

```
IF risk_raw ≥ 0.8              → "HIGH RISK" (overrides sentiment)
ELSE IF risk_raw ≥ 0.5 & < 0.8 → "MODERATE RISK" (overrides sentiment)
ELSE                           → Keep sentiment-based label
```

### Decision Flow Chart
```
Article → Check confidence → Low? → "NEUTRAL"
                          ↓
                      Check sentiment → Assign label
                          ↓
                      Check risk_raw → Override if high
                          ↓
                    Final impact_assessment
```

### Examples

#### Example 1: No Risk Override
**Input**:
- sentiment = 0.5
- confidence = 80
- risk_raw = 0.3

**Process**:
1. confidence (80) ≥ 20 ✓
2. sentiment (0.5) ≥ 0.4 → **"POSITIVE"**
3. risk_raw (0.3) < 0.5 → No override

**Output**: `impact_assessment = "POSITIVE"`

#### Example 2: Risk Override
**Input**:
- sentiment = -0.3
- confidence = 90
- risk_raw = 0.9

**Process**:
1. confidence (90) ≥ 20 ✓
2. sentiment (-0.3) ≤ -0.1 → "SLIGHTLY NEGATIVE"
3. risk_raw (0.9) ≥ 0.8 → **Override!**

**Output**: `impact_assessment = "HIGH RISK"`

### Possible Output Values
- `"BULLISH"`
- `"POSITIVE"`
- `"SLIGHTLY POSITIVE"`
- `"NEUTRAL"`
- `"SLIGHTLY NEGATIVE"`
- `"NEGATIVE"`
- `"BEARISH"`
- `"MODERATE RISK"`
- `"HIGH RISK"`

---

## 6. Impact Score (impact_score)

### Purpose
Convert text labels from impact_assessment into numeric values for final score calculation.

### Complete Mapping Table
| impact_assessment | impact_score | Type | Interpretation |
|-------------------|--------------|------|----------------|
| `"BULLISH"` | **+2.0** | Sentiment | Strongest positive signal |
| `"POSITIVE"` | **+1.0** | Sentiment | Moderate positive |
| `"SLIGHTLY POSITIVE"` | **+0.5** | Sentiment | Mild positive |
| `"NEUTRAL"` | **0.0** | Sentiment | No directional signal |
| `"SLIGHTLY NEGATIVE"` | **-0.5** | Sentiment | Mild negative |
| `"NEGATIVE"` | **-1.0** | Sentiment | Moderate negative |
| `"BEARISH"` | **-2.0** | Sentiment | Strongest negative signal |
| `"MODERATE RISK"` | **-1.5** | Risk | Risk-based penalty |
| `"HIGH RISK"` | **-3.0** | Risk | Critical risk - strongest penalty |

### Scale Visualization
```
+2.0  ████████████████████  BULLISH (Best)
+1.0  ██████████  POSITIVE
+0.5  █████  SLIGHTLY POSITIVE
 0.0  ─  NEUTRAL
-0.5  █████  SLIGHTLY NEGATIVE
-1.0  ██████████  NEGATIVE
-1.5  ███████████████  MODERATE RISK
-2.0  ████████████████████  BEARISH
-3.0  ██████████████████████████████  HIGH RISK (Worst)
```

### Key Observations
- Risk assessments carry more weight than pure sentiment
- `"HIGH RISK"` (-3.0) is more severe than `"BEARISH"` (-2.0)
- Positive sentiment ranges from 0 to +2.0
- Negative/risk ranges from 0 to -3.0 (wider range)

### Example
```
impact_assessment = "HIGH RISK"
→ impact_score = -3.0
```

---

## 7. Final Composite Score (final_score)

### Purpose
Create single unified score that balances impact and risk, weighted by confidence.

### Formula
```
final_score = (impact_score × conf_norm) - (risk_raw × (1 - conf_norm))
```

### Two Components

#### Component 1: Impact Term (Positive/Negative Signal)
```
impact_score × conf_norm
```
- Weights impact by confidence
- High confidence = stronger signal
- Positive impacts boosted, negative impacts amplified

#### Component 2: Risk Penalty (Uncertainty Cost)
```
risk_raw × (1 - conf_norm)
```
- Penalizes uncertainty
- Low confidence = higher risk penalty
- High confidence = reduced risk penalty (we trust the assessment)

### Visual Formula Breakdown
```
final_score = (impact_score × conf_norm) - (risk_raw × (1 - conf_norm))
              ───────────────────────────   ──────────────────────────────
              Confidence-weighted signal    Uncertainty-weighted penalty
```

### Complete Example Calculation

**Input**:
- impact_score = -3.0 (HIGH RISK)
- conf_norm = 0.567
- risk_raw = 0.9

**Step-by-Step**:
```
final_score = (impact_score × conf_norm) - (risk_raw × (1 - conf_norm))

Step 1: Calculate impact term
= -3.0 × 0.567
= -1.701

Step 2: Calculate risk penalty
= 0.9 × (1 - 0.567)
= 0.9 × 0.433
= 0.390

Step 3: Combine (subtract penalty)
= -1.701 - 0.390
= -2.091
```

**Interpretation**: Strong negative signal (-2.09) due to high risk with moderate confidence.

### Score Interpretation Guide

| final_score Range | Interpretation | Trading Signal |
|-------------------|----------------|----------------|
| **+1.5 to +2.0** | Very strong positive | Strong buy |
| **+0.5 to +1.5** | Positive | Buy |
| **0 to +0.5** | Slightly positive | Weak buy |
| **-0.5 to 0** | Slightly negative | Weak sell |
| **-1.5 to -0.5** | Negative | Sell |
| **-3.0 to -1.5** | Very strong negative | Strong sell / avoid |

### Example Scenarios

#### Scenario 1: High Confidence Bullish
```
impact_score = 2.0 (BULLISH)
conf_norm = 0.9
risk_raw = 0.1

final_score = (2.0 × 0.9) - (0.1 × 0.1)
            = 1.8 - 0.01
            = 1.79 ✅ Strong buy signal
```

#### Scenario 2: Low Confidence Negative
```
impact_score = -1.0 (NEGATIVE)
conf_norm = 0.3
risk_raw = 0.7

final_score = (-1.0 × 0.3) - (0.7 × 0.7)
            = -0.3 - 0.49
            = -0.79 ⚠️ Uncertainty penalty amplifies negative
```

#### Scenario 3: High Risk Override
```
impact_score = -3.0 (HIGH RISK)
conf_norm = 0.95
risk_raw = 0.9

final_score = (-3.0 × 0.95) - (0.9 × 0.05)
            = -2.85 - 0.045
            = -2.895 🔴 Critical risk - strong sell
```

---

## 8. Sentiment Confidence Score

### Purpose
Simple confidence-weighted sentiment for direct comparison across articles.

### Formula
```
sentiment_confidence_score = sentiment × conf_norm
```

### Why Simpler Than final_score?
- No risk penalty applied
- Just weights raw sentiment by confidence
- Useful for pure sentiment analysis without risk overlay

### Example
**Input**:
- sentiment = -0.8
- conf_norm = 0.957

**Calculation**:
```
sentiment_confidence_score = -0.8 × 0.957
                           = -0.766
```

**Interpretation**: Strong confident negative sentiment.

### Comparison: final_score vs sentiment_confidence_score

| Metric | Includes Sentiment | Includes Risk | Includes Uncertainty Penalty |
|--------|-------------------|---------------|------------------------------|
| `sentiment_confidence_score` | ✅ | ❌ | ❌ |
| `final_score` | ✅ | ✅ | ✅ |

### When to Use Each
- **sentiment_confidence_score**: Pure sentiment analysis, comparing article tone
- **final_score**: Complete risk-adjusted signal for trading decisions

---

## 9. Complete Example Flow

### Input Article
```
Title: "Tesla files for bankruptcy protection after fraud investigation"
content: "Tesla Inc. announced today it has filed for Chapter 11 bankruptcy..."
confidence: 750
sentiment: -0.85
```

### Step-by-Step Calculations

#### Step 1: Normalize Confidence
```
conf_norm = log(1 + 750) / log(1 + 1000)
          = log(751) / log(1001)
          = 6.621 / 6.909
          = 0.957
```

#### Step 2: Classify Risk Level
```
Check conditions:
✓ Contains "bankruptcy" (risk keyword)
✓ Contains "fraud" (risk keyword)
✓ sentiment (-0.85) < 0

→ risk_level = "high_risk"
```

#### Step 3: Map to Risk Raw Score
```
risk_level = "high_risk"
→ risk_raw = 0.9
```

#### Step 4: Calculate Adjusted Risk
```
risk_adj = (0.9 × 0.8) + (0.9 × 0.957 × 0.2)
         = 0.72 + 0.172
         = 0.892
```

#### Step 5: Determine Impact Assessment
```
Step 5A: Sentiment-based
confidence (750) ≥ 20 ✓
sentiment (-0.85) ≤ -0.7 → "BEARISH"

Step 5B: Risk override
risk_raw (0.9) ≥ 0.8 ✓
→ Override to "HIGH RISK"

→ impact_assessment = "HIGH RISK"
```

#### Step 6: Map to Impact Score
```
impact_assessment = "HIGH RISK"
→ impact_score = -3.0
```

#### Step 7: Calculate Final Score
```
final_score = (-3.0 × 0.957) - (0.9 × (1 - 0.957))
            = (-3.0 × 0.957) - (0.9 × 0.043)
            = -2.871 - 0.039
            = -2.910
```

#### Step 8: Calculate Sentiment Confidence
```
sentiment_confidence_score = -0.85 × 0.957
                           = -0.813
```

### Final Output Summary
```
conf_norm: 0.957
risk_level: "high_risk"
risk_raw: 0.9
risk_adj: 0.892
impact_assessment: "HIGH RISK"
impact_score: -3.0
final_score: -2.910 ⚠️ CRITICAL NEGATIVE SIGNAL
sentiment_confidence_score: -0.813
```

### Trading Interpretation
🔴 **STRONG SELL SIGNAL**
- Very high confidence (0.957)
- Critical risk level (bankruptcy + fraud)
- Final score -2.91 (extreme negative)
- Avoid this stock completely

---

## Quick Reference Table

| Field | Formula/Logic | Range | Purpose |
|-------|--------------|-------|---------|
| **conf_norm** | `log(1+conf) / log(1001)` | 0 - 1 | Normalize confidence |
| **risk_level** | Keyword + sentiment rules | Categorical | Classify risk |
| **risk_raw** | Risk level mapping | 0.1 - 0.9 | Numeric risk base |
| **risk_adj** | `(risk_raw×0.8) + (risk_raw×conf_norm×0.2)` | 0.1 - 0.9 | Adjusted risk |
| **impact_assessment** | Sentiment + risk override | Text labels | Human-readable impact |
| **impact_score** | Label to number mapping | -3.0 to +2.0 | Numeric impact |
| **final_score** | `(impact×conf) - (risk×(1-conf))` | ~-3.0 to +2.0 | Trading signal |
| **sentiment_conf** | `sentiment × conf_norm` | -1.0 to +1.0 | Pure sentiment |

---

## FAQs

### Q: Why use logarithmic scaling for confidence?
**A**: Prevents saturation. The difference between conf=50 and conf=100 is more meaningful than conf=900 and conf=950.

### Q: Why do risk assessments override sentiment?
**A**: Risk indicators (bankruptcy, fraud) are critical events that override normal sentiment patterns.

### Q: Why does final_score have an uncertainty penalty?
**A**: Low confidence articles shouldn't drive trading decisions, so we penalize them.

### Q: What's a good final_score threshold for trading?
**A**: 
- Buy: final_score > +0.5
- Sell: final_score < -0.5
- Hold: -0.5 to +0.5

### Q: Why is HIGH RISK scored at -3.0 instead of -2.0?
**A**: Risk events are more severe than sentiment. A bankruptcy is worse than just "bearish" news.

---

## Last Updated
February 3, 2026

**Contact**: For questions about these calculations, refer to Transform.py implementation or project documentation.
