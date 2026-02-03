import pandas as pd
from sklearn.metrics import accuracy_score, precision_recall_fscore_support, confusion_matrix
from rouge_score import rouge_scorer

# -----------------------------
# LOAD DATA
# -----------------------------
gt = pd.read_csv("ground_truth.csv")
pred = pd.read_csv("predictions.csv")

df = pd.merge(gt, pred, on="doc_id")

print("Total Evaluation Samples:", len(df))

# -----------------------------
# CATEGORY EVALUATION
# -----------------------------
print("\n=== CATEGORY EVALUATION ===")

y_true_cat = df["true_category"]
y_pred_cat = df["category"]

acc = accuracy_score(y_true_cat, y_pred_cat)
prec, rec, f1, _ = precision_recall_fscore_support(y_true_cat, y_pred_cat, average="weighted")

print("Accuracy:", acc)
print("Precision:", prec)
print("Recall:", rec)
print("F1 Score:", f1)

# -----------------------------
# SENTIMENT EVALUATION
# -----------------------------
print("\n=== SENTIMENT EVALUATION ===")

def normalize_sentiment(x):
    x = str(x).upper()
    if "POS" in x:
        return "POSITIVE"
    elif "NEG" in x:
        return "NEGATIVE"
    else:
        return "NEUTRAL"

df["pred_sentiment_norm"] = df["impact_assessment"].apply(normalize_sentiment)
df["true_sentiment_norm"] = df["true_sentiment"].str.upper()

y_true_sent = df["true_sentiment_norm"]
y_pred_sent = df["pred_sentiment_norm"]

acc_s = accuracy_score(y_true_sent, y_pred_sent)
prec_s, rec_s, f1_s, _ = precision_recall_fscore_support(y_true_sent, y_pred_sent, average="weighted")

print("Accuracy:", acc_s)
print("Precision:", prec_s)
print("Recall:", rec_s)
print("F1 Score:", f1_s)

print("\nConfusion Matrix:")
print(confusion_matrix(y_true_sent, y_pred_sent))

# -----------------------------
# KEY PHRASE EVALUATION
# -----------------------------
print("\n=== KEY PHRASE EVALUATION ===")

def split_phrases(text):
    return set(str(text).lower().replace(",", " ").split())

precisions = []
recalls = []

for _, row in df.iterrows():
    true_set = split_phrases(row["true_key_phrases"])
    pred_set = split_phrases(row["key_phrases"])

    if len(pred_set) == 0:
        continue

    correct = len(true_set & pred_set)

    precision = correct / len(pred_set) if len(pred_set) else 0
    recall = correct / len(true_set) if len(true_set) else 0

    precisions.append(precision)
    recalls.append(recall)

print("Avg Precision:", sum(precisions)/len(precisions))
print("Avg Recall:", sum(recalls)/len(recalls))

# -----------------------------
# SUMMARY EVALUATION (ROUGE)
# -----------------------------
print("\n=== SUMMARY ROUGE EVALUATION ===")

scorer = rouge_scorer.RougeScorer(['rouge1', 'rouge2', 'rougeL'], use_stemmer=True)

r1 = []
r2 = []
rl = []

for _, row in df.iterrows():
    scores = scorer.score(str(row["true_summary"]), str(row["summary"]))
    r1.append(scores["rouge1"].fmeasure)
    r2.append(scores["rouge2"].fmeasure)
    rl.append(scores["rougeL"].fmeasure)

print("ROUGE-1:", sum(r1)/len(r1))
print("ROUGE-2:", sum(r2)/len(r2))
print("ROUGE-L:", sum(rl)/len(rl))

print("\n=== EVALUATION COMPLETE ===")
