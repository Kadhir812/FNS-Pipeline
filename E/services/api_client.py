import requests
from .config import MARKETAUX_API_KEY


def fetch_news():
    if not MARKETAUX_API_KEY:
        print("MARKETAUX_API_KEY is missing. Set it in E/.env")
        return []

    url = "https://api.marketaux.com/v1/news/all"
    params = {
        "api_token": MARKETAUX_API_KEY,
        "categories": "finance",
        "language": "en",
        "countries": "us",
    }

    response = requests.get(url, params=params, timeout=10)
    print("DEBUG: Raw API response:")
    print(response.text[:500] + "..." if len(response.text) > 500 else response.text)

    try:
        data = response.json()
        if data and "data" in data and len(data["data"]) > 0:
            first_article = data["data"][0]
            if "entities" in first_article and len(first_article["entities"]) > 0:
                first_entity = first_article["entities"][0]
                print(
                    f"DEBUG: First entity example - Symbol: {first_entity.get('symbol')}, Name: {first_entity.get('name')}"
                )
    except ValueError:
        print("Not a valid JSON response!")
        print(response.text)
        return []

    return data.get("data", [])
