import redis

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

categories = [
    "A","B","C","D","E","F","G","H","I","K","L","M","N"
]

for cat in categories:
    key = f"category_keywords_{cat}"
    count = r.scard(key)
    print(f"{key}: {count} keywords")
