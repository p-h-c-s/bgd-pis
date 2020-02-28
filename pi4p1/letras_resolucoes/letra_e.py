import redis
import json
import heapq
from operator import itemgetter

r = redis.Redis(decode_responses=True)

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

def getTopN_E(values):
  print('\n10 produtos com a maior média de avaliações úteis positivas')
  print('-----------------------------------------------------')
  topN = heapq.nlargest(10, values, itemgetter(1))
  for prd in topN:
    print('ASIN: {} com média {}'.format(prd[0], prd[1]))
  print('-----------------------------------------------------')


def calcAverage(reviews):
  avgRating = 0
  count = 1
  if len(reviews) > 0:
    for r in reviews:
      if(int(r['rating']) >= 5):
        count += 1
        avgRating += int(r['helpful'])
    return avgRating/count
  return 0.0

groupSet = []
cursor = '0'
while cursor != 0:
  cursor, keys = r.scan(cursor=cursor, count=100)
  for key in keys:
    value = getProductByKey(key, r)
    heapq.heappush(groupSet, (value['ASIN'], calcAverage(value['reviews'])))

getTopN_E(groupSet)