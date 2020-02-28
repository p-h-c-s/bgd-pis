import redis
import json

r = redis.Redis()

data = json.loads(r.get('1568581718').decode('utf-8'))