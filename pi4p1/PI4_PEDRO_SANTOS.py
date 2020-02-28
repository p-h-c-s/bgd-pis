import redis


r = redis.Redis()

print(r.ping())