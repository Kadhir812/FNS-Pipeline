import json
import re
import time
import requests
from langchain_ollama import OllamaLLM
from langchain.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnableLambda
from .config import OLLAMA_MODEL, OLLAMA_URL
from .logging_setup import logger, fallback_logger
from .classification_service import enhanced_classify_news_category


def clean_text(text):
    if text:
        return re.sub(r"<[^>]+>", "", text)
    return ""


def clean_ollama_response(response):
    if not response:
        return "N/A"

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
            after_entities = re.search(r"(?:entities|phrases):\s*(.+)", response, re.IGNORECASE)
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

        cleaned_response = clean_ollama_response(full_reply.strip())
        return cleaned_response if cleaned_response else "N/A"
    except requests.exceptions.RequestException as error:
        print(f"Ollama request error: {error}")
        return "N/A"


def get_ollama_model():
    return OllamaLLM(model=OLLAMA_MODEL, base_url="http://localhost:11434")


def create_news_processing_chain():
    llm = get_ollama_model()
    output_parser = StrOutputParser()

    key_phrases_template = (
        "Extract financial entities: tickers, amounts, percentages, companies, dates, actions.\n"
        "Format: lowercase, comma-separated, max 15 items.\n\n"
        "Text: {text}\n\n"
        "Entities:"
    )
    key_phrases_prompt = PromptTemplate(template=key_phrases_template, input_variables=["text"])
    key_phrases_chain = key_phrases_prompt | llm | output_parser

    summary_template = (
        "One sentence summary: Subject + Action + Impact (<15 words).\n\n"
        "Text: {text}\n\n"
        "Summary:"
    )
    summary_prompt = PromptTemplate(template=summary_template, input_variables=["text"])
    summary_chain = summary_prompt | llm | output_parser

    def process_chains(inputs):
        text = inputs["text"]
        return {
            "key_phrases": key_phrases_chain.invoke({"text": text}),
            "summary": summary_chain.invoke({"text": text}),
        }

    return RunnableLambda(process_chains)


try:
    NEWS_PROCESSING_CHAIN = create_news_processing_chain()
    USE_LANGCHAIN = True
except Exception as error:
    USE_LANGCHAIN = False
    NEWS_PROCESSING_CHAIN = None
    print(f"LangChain initialization failed: {error}")


def fallback_to_individual_processing(payload, text, article_num):
    logger.warning(f"Article {article_num}: Using FALLBACK path with direct Ollama calls")

    try:
        key_phrases_prompt = (
            "Extract financial entities: tickers, amounts, percentages, companies, dates, actions.\n"
            "Format: lowercase, comma-separated, max 15 items.\n"
            "Text: + text\n"
            "Entities:"
        )
        payload["key_phrases"] = ollama_query(key_phrases_prompt, text)
    except Exception as error:
        logger.error(f"Article {article_num}: Direct Ollama - Key phrases FAILED: {str(error)}")
        payload["key_phrases"] = "N/A"

    try:
        summary_prompt = (
            "One sentence summary: Subject + Action + Impact (<15 words).\n"
            "Text: + text\n"
            "Summary:"
        )
        payload["summary"] = ollama_query(summary_prompt, text)
    except Exception as error:
        logger.error(f"Article {article_num}: Direct Ollama - Summary FAILED: {str(error)}")
        payload["summary"] = "Financial news update"


def enrich_news(payload, article_num, total_articles):
    summary = payload.get("summary", "").lower()
    content = payload.get("content", "").lower()

    positive_keywords = ["beat", "surge", "growth", "bullish", "strong", "record", "positive", "rally", "gain"]
    negative_keywords = ["risk", "fall", "drop", "loss", "bearish", "struggle", "decline", "negative", "warning"]

    if any(word in summary or word in content for word in positive_keywords):
        payload["sentiment"] = max(payload.get("sentiment", 0.0) or 0.0, 0.7)
    elif any(word in summary or word in content for word in negative_keywords):
        payload["sentiment"] = min(payload.get("sentiment", 0.0) or 0.0, -0.7)

    text = clean_text(payload.get("content", "") or payload.get("description", ""))
    if not text:
        text = payload.get("title", "")

    print(f"Processing Article {article_num}/{total_articles}")

    enhanced_classify_news_category(text, payload, article_num)

    if USE_LANGCHAIN and NEWS_PROCESSING_CHAIN is not None:
        try:
            logger.info(f"Article {article_num}: Using LangChain path")
            results = NEWS_PROCESSING_CHAIN.invoke({"text": text})
            payload["key_phrases"] = clean_ollama_response(results.get("key_phrases", "N/A"))
            payload["summary"] = clean_ollama_response(results.get("summary", "Financial news update"))
        except Exception as error:
            logger.error(f"Article {article_num}: LangChain FAILED - Error: {str(error)}")
            fallback_logger.warning(
                f"PROCESSING_FALLBACK - Article: {article_num}, Reason: LangChain_Error, Method: Direct_Ollama, Error: {str(error)}"
            )
            fallback_to_individual_processing(payload, text, article_num)
    else:
        logger.info(f"Article {article_num}: Using direct Ollama path (LangChain not available)")
        fallback_logger.info(
            f"PROCESSING_DIRECT - Article: {article_num}, Reason: LangChain_Unavailable, Method: Direct_Ollama"
        )
        fallback_to_individual_processing(payload, text, article_num)

    return payload
