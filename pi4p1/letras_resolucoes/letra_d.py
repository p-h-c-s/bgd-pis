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

def getTopN(groups):
  for group in groups:
    print('\n10 produtos líderes de venda (menor salesrank) em {}:'.format(group))
    print('-----------------------------------------------------')
    topN = heapq.nsmallest(10, groupSet[group], itemgetter(1))
    for prd in topN:
      print('ASIN: {} com salesrank {}'.format(prd[0], prd[1]))
    print('-----------------------------------------------------')

groupSet = {}
cursor = '0'
while cursor != 0:
  cursor, keys = r.scan(cursor=cursor, count=100)
  for key in keys:
    value = getProductByKey(key, r)
    if value['group'] not in groupSet and value['salesrank'] != -1:
      # initialize heap then push to it
      groupSet[value['group']] = []
      heapq.heappush(groupSet[value['group']], (value['ASIN'], value['salesrank']))
    elif(value['salesrank'] != -1):
      heapq.heappush(groupSet[value['group']], (value['ASIN'], value['salesrank']))

print('Letra d)')
getTopN(groupSet)