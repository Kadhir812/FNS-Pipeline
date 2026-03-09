import hashlib
import json


def get_doc_id(article):
    key = (article.get("title", "") or "") + (article.get("snippet", "") or "")
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def load_existing_doc_ids(path):
    doc_ids = set()
    try:
        with open(path, "r", encoding="utf-8") as file:
            for line in file:
                try:
                    obj = json.loads(line)
                    if "doc_id" in obj:
                        doc_ids.add(obj["doc_id"])
                except Exception:
                    continue
    except FileNotFoundError:
        pass

    return doc_ids


def append_payloads_to_archive(path, payloads):
    with open(path, "a", encoding="utf-8") as file:
        for payload in payloads:
            file.write(json.dumps(payload, ensure_ascii=False) + "\n")
