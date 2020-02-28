import redis
import json
import heapq
from operator import itemgetter

r = redis.Redis(decode_responses=True)

# produto a procurar os similares com maior venda
product = '0486220125'

def getProductByKey(key, redisInstance):
  raw_data = redisInstance.get(key)
  if (raw_data != None):
    return json.loads(raw_data)
  return None

product_data = getProductByKey(product, r)
product_reviews = sorted(product_data['reviews'], key=itemgetter('date'))

ratings = []
for index in range(0,len(product_reviews)):
    avg = 0
    for j in range(0,index+1):
        avg += product_reviews[j]['rating']
    avg = avg/(index+1)
    ratings.append((product_reviews[index]['date'],avg))

print('\nEvolução diária das médias de avaliação de {}'.format(product))
print('-'*35)
for i in ratings:
  if(len(str(i)) < 33):
    i = str(i)
    i += ' ' * (33-len(i))
  print('|{}|'.format(i))
print('-'*35)