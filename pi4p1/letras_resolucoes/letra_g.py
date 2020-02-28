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

def getTopN_G(groups):
  for group in groups:
    print('\n10 clientes com mais comentários em {}:'.format(group))
    print('-----------------------------------------------------')
    topN = heapq.nlargest(10, groupSet[group], key=groupSet[group].get)
    for prd in topN:
      print('Customer: {} com {} comentarios'.format(prd, groupSet[group][prd]))
    print('-----------------------------------------------------')

groupSet = {}
cursor = '0'
while cursor != 0:
  cursor, keys = r.scan(cursor=cursor, count=100)
  for key in keys:
    value = getProductByKey(key, r)
    if value['group'] not in groupSet :
      groupSet[value['group']] = {}
    for review in value['reviews']:
      if review['customer'] not in groupSet[value['group']]:
        groupSet[value['group']][review['customer']] = 1
      else:
        groupSet[value['group']][review['customer']] += 1

print('Letra d)')
getTopN_G(groupSet)