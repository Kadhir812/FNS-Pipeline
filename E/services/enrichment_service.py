import json
import re
import time
import requests
from langchain_ollama import OllamaLLM
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda
from .config import OLLAMA_MODEL, OLLAMA_URL
from .logging_setup import logger, fallback_logger
from .classification_service import enhanced_classify_news_category


# ============================================================
# TEXT HELPERS
# ============================================================
def clean_text(text):
    if text:
        return re.sub(r"<[^>]+>", "", text)
    return ""


def build_extraction_text(payload):
    """
    Build minimal high-signal text for Ollama.
    Title + first 200 chars of description — fast, accurate enough.
    Skips full content — too long, mostly noise for small models.
    """
    title       = (payload.get("title",       "") or "").strip()
    description = (payload.get("description", "") or "").strip()
    combined    = f"{title}. {description[:200]}" if description else title
    return combined[:400]  # hard cap — keeps inference fast


def clean_ollama_response(response):
    if not response:
        return "N/A"

    # Strip DeepSeek thinking tags
    cleaned = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL)
    cleaned = re.sub(r"<[^>]+>", "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip()

    if not cleaned or len(cleaned) > 200:
        if re.search(r"\b[ABCDE]\b", response):
            category_match = re.search(r"\b([ABCDE])\b", response)
            if category_match:
                return category_match.group(1)

        for pattern in ["POSITIVE", "NEGATIVE", "NEUTRAL"]:
            if pattern.upper() in response.upper():
                return pattern

        if "," in response:
            after_entities = re.search(
                r"(?:entities|phrases):\s*(.+)", response, re.IGNORECASE
            )
            if after_entities:
                phrases = after_entities.group(1).strip()
                if len(phrases) < 500:
                    return phrases

        sentences = re.findall(r"[A-Z][^.!?]*[.!?]", response)
        if sentences:
            for sentence in sentences:
                if 5 < len(sentence) < 150:
                    return sentence.strip()

    return cleaned if cleaned else "N/A"


def validate_key_phrases(raw_phrases, source_text):
    """
    Remove any phrase not found in source text.
    Hard filter against hallucinated values from small models.
    """
    if not raw_phrases or raw_phrases.strip().lower() == "n/a":
        return "N/A"

    source_lower = source_text.lower()
    phrases      = [p.strip() for p in raw_phrases.split(",")]
    validated    = []

    for phrase in phrases:
        phrase_clean = phrase.lower().strip()
        if len(phrase_clean) < 2:
            continue
        if phrase_clean == "n/a":
            continue
        # Keep phrase only if at least one meaningful word exists in source
        words = [w for w in phrase_clean.split() if len(w) > 3]
        if words and any(w in source_lower for w in words):
            validated.append(phrase.strip())

    return ", ".join(validated) if validated else "N/A"


# ============================================================
# DIRECT OLLAMA QUERY (fallback path)
# ============================================================
def ollama_query(prompt, text):
    try:
        response = requests.post(
            OLLAMA_URL,
            json={"model": OLLAMA_MODEL, "prompt": prompt.replace("+ text", text)},
            stream=True,
        )

        if response.status_code != 200:
            print(f"Ollama error: {response.status_code}")
            return "N/A"

        full_reply = ""
        for line in response.iter_lines(decode_unicode=True):
            if line:
                try:
                    data = json.loads(line)
                    full_reply += data.get("response", "")
                except json.JSONDecodeError:
                    continue

        cleaned = clean_ollama_response(full_reply.strip())
        return cleaned if cleaned else "N/A"

    except requests.exceptions.RequestException as error:
        print(f"Ollama request error: {error}")
        return "N/A"


# ============================================================
# LANGCHAIN CHAIN
# ✅ Short prompts — faster inference on CPU
# ✅ Explicit no-invention instruction
# ============================================================
def get_ollama_model():
    return OllamaLLM(model=OLLAMA_MODEL, base_url="http://ollama:11434")


def create_news_processing_chain():
    llm           = get_ollama_model()
    output_parser = StrOutputParser()

    # ✅ Short, constrained — ~28 tokens instruction
    key_phrases_template = (
        "Extract ONLY entities present in this text. No invented values.\n"
        "Format: comma-separated, max 8 items.\n\n"
        "Text: {text}\n\nEntities:"
    )
    key_phrases_prompt = PromptTemplate(
        template=key_phrases_template, input_variables=["text"]
    )
    key_phrases_chain = key_phrases_prompt | llm | output_parser

    # ✅ Short, factual — ~20 tokens instruction
    summary_template = (
        "Summarize in one sentence using only facts from the text.\n\n"
        "Text: {text}\n\nSummary:"
    )
    summary_prompt = PromptTemplate(
        template=summary_template, input_variables=["text"]
    )
    summary_chain = summary_prompt | llm | output_parser

    def process_chains(inputs):
        text = inputs["text"]
        return {
            "key_phrases": key_phrases_chain.invoke({"text": text}),
            "summary":     summary_chain.invoke({"text": text}),
        }

    return RunnableLambda(process_chains)


try:
    NEWS_PROCESSING_CHAIN = create_news_processing_chain()
    USE_LANGCHAIN         = True
except Exception as error:
    USE_LANGCHAIN         = False
    NEWS_PROCESSING_CHAIN = None
    print(f"LangChain initialization failed: {error}")


# ============================================================
# FALLBACK — direct Ollama calls
# ✅ Same short prompts as LangChain path
# ============================================================
def fallback_to_individual_processing(payload, text, article_num):
    logger.warning(f"Article {article_num}: Using FALLBACK path with direct Ollama calls")

    try:
        key_phrases_prompt = (
            "Extract ONLY entities present in this text. No invented values.\n"
            "Format: comma-separated, max 8 items.\n"
            "Text: + text\n"
            "Entities:"
        )
        raw = ollama_query(key_phrases_prompt, text)
        payload["key_phrases"] = validate_key_phrases(raw, text)
    except Exception as error:
        logger.error(f"Article {article_num}: Direct Ollama - Key phrases FAILED: {str(error)}")
        payload["key_phrases"] = "N/A"

    try:
        summary_prompt = (
            "Summarize in one sentence using only facts from the text.\n"
            "Text: + text\n"
            "Summary:"
        )
        payload["summary"] = ollama_query(summary_prompt, text)
    except Exception as error:
        logger.error(f"Article {article_num}: Direct Ollama - Summary FAILED: {str(error)}")
        payload["summary"] = "Financial news update"


# ============================================================
# MAIN ENRICHMENT
# ✅ Uses title+description only for Ollama — faster inference
# ✅ Validates key phrases against source — removes hallucinations
# ✅ Removed keyword sentiment override — trusts MarketAux + FinBERT
# ============================================================
def enrich_news(payload, article_num, total_articles):

    # ── Classification text — full content for better category signal
    classification_text = clean_text(
        payload.get("content", "") or payload.get("description", "")
    )
    if not classification_text:
        classification_text = payload.get("title", "")

    # ── Extraction text — short, fast, title+desc only for Ollama
    extraction_text = build_extraction_text(payload)

    print(f"Processing Article {article_num}/{total_articles} "
          f"[classify={len(classification_text)}chars, "
          f"extract={len(extraction_text)}chars]")

    # ── Category classification — uses full content
    enhanced_classify_news_category(classification_text, payload, article_num)

    # ── LLM enrichment — uses short extraction text
    if USE_LANGCHAIN and NEWS_PROCESSING_CHAIN is not None:
        try:
            logger.info(f"Article {article_num}: Using LangChain path")
            results = NEWS_PROCESSING_CHAIN.invoke({"text": extraction_text})

            raw_phrases        = clean_ollama_response(results.get("key_phrases", "N/A"))
            # ✅ Validate — removes hallucinated values
            payload["key_phrases"] = validate_key_phrases(raw_phrases, extraction_text)
            payload["summary"]     = clean_ollama_response(
                results.get("summary", "Financial news update")
            )

            logger.info(
                f"Article {article_num}: key_phrases={payload['key_phrases'][:60]}"
            )

        except Exception as error:
            logger.error(f"Article {article_num}: LangChain FAILED - {str(error)}")
            fallback_logger.warning(
                f"PROCESSING_FALLBACK - Article: {article_num}, "
                f"Reason: LangChain_Error, Method: Direct_Ollama, Error: {str(error)}"
            )
            fallback_to_individual_processing(payload, extraction_text, article_num)
    else:
        logger.info(f"Article {article_num}: Using direct Ollama path")
        fallback_logger.info(
            f"PROCESSING_DIRECT - Article: {article_num}, "
            f"Reason: LangChain_Unavailable, Method: Direct_Ollama"
        )
        fallback_to_individual_processing(payload, extraction_text, article_num)

    return payload