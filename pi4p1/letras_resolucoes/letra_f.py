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

def getTopN_F(groups):
  print('\n5 categorias com maior média de avaliações úteis positivas')
  print('-----------------------------------------------------')
  topN = heapq.nlargest(5, groupSet, itemgetter(1))
  for prd in topN:
    print('Category: {} com média: {}'.format(prd, groups[prd]))
  print('-----------------------------------------------------')

def getLeafCat(categories):
  if (len(categories) > 0):
    return categories[0].strip().split('|')[1]
  return None

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

groupSet = {}
cursor = '0'
while cursor != 0:
  cursor, keys = r.scan(cursor=cursor, count=100)
  for key in keys:
    value = getProductByKey(key, r)
    categories = value['categories']
    for cat in categories:
      if cat not in groupSet and cat != None:
        groupSet[cat] = calcAverage(value['reviews'])
      elif(cat != None):
        groupSet[cat] = (calcAverage(value['reviews']) + groupSet[cat])/2

print('Letra f)')
getTopN_F(groupSet)