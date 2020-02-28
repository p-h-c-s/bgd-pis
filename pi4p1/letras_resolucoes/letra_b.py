import redis
import json

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
product_salesrank = product_data['salesrank']
similars = product_data['similar']
similars_salesrank = [getSpecificFields(getProductByKey(similar, r), ['ASIN', 'salesrank']) for similar in similars]
similars_salesrank = filter(lambda x: x != None, similars_salesrank)

print('\nProdutos similares a {} com mais vendas. (salesrank menor)'.format(product))
for similar in similars_salesrank:
  if product_salesrank > similar['salesrank']:
    print(json.dumps(similar, indent=4))