import redis
import json
import heapq
from operator import itemgetter

r = redis.Redis(decode_responses=True)

# produto a procurar os similares com maior venda
product = 'B00004R99S'

def getProductByKey(key, redisInstance):
  raw_data = redisInstance.get(key)
  if (raw_data != None):
    return json.loads(raw_data)
  return None

def getSpecificFields(product, fields=['ASIN']):
  if (product != None):
    new_prd = {}
    for field in fields:
      new_prd[field] = product[field] 
    return new_prd
  return None

product_data = getProductByKey(product, r)
product_reviews = product_data['reviews']
product_reviews_max = sorted(product_reviews, key=itemgetter('helpful', 'rating'), reverse=True)
product_reviews_min = sorted(
                      sorted(product_reviews, key=itemgetter('helpful'), reverse=True), key=itemgetter('rating'),
                      reverse=False)

print('-------------------------\nPara o produto {}:'.format(product))
print('-------------------------\n5 comentários mais úteis e com maior avaliação: ')
for review in product_reviews_max[:5]:
  print(review)
print('--------------------------\n5 comentários mais úteis com menor avaliação: ')
for review in product_reviews_min[:5]:
  print(review)
