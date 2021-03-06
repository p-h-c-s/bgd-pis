import functools
import pymongo
import sys
from operator import add

connector = pymongo.MongoClient("mongodb://localhost:27017/")
db = connector['database']

products = db['products']

def create_obj(array):
  obj = {}
  obj[array[0][0]] = int(array[0][1].strip())  # Id
  obj[array[1][0]] = array[1][1].strip()  # ASIN

  title = ''
  for til in array[2][1:-2]:
    title += til
  title += array[2][-1]
  obj[array[2][0].strip()] = title  # title

  obj[array[3][0].strip()] = array[3][1].strip()  # group
  obj[array[4][0].strip()] = int(array[4][1].strip())  # salesrank

  obj[array[5][0].strip()] = array[5][1][2:].split()  # similars

  n_categories = int(array[6][1].strip())
  obj[array[6][0].strip()] = []  # categories
  for i in range(0, n_categories):
    obj[array[6][0].strip()].append(array[i+7][0].strip())

  skip = 8+n_categories
  obj['reviews'] = []
  for i in range(skip, (len(array)-1)):
    date = array[i][0].strip().split()[0].split('-')
    if len(date[1]) == 1:
      date[1] = '0' + date[1]
    if len(date[2]) == 1:
      date[2] = '0' + date[2]
    obj['reviews'].append({
      'date': '-'.join(date),
      'customer': array[i][1].strip().split()[0],
      'rating': int(array[i][1].strip().split()[2]),
      'votes': int(array[i][1].strip().split()[4]),
      'helpful': int(array[i][1].strip().split()[6])
    })

  products.insert_one(obj)


def delimited(file, delimiter, bufsize=4096):
  buf = ''
  while True:
    newbuf = file.read(bufsize)
    if not newbuf:
      yield buf
      return
    buf += newbuf
    lines = buf.split(delimiter)
    for line in lines[:-1]:
      yield line
    buf = lines[-1]

products.delete_many({})
data = list(delimited(open('../pi2pa/data/amazon-meta.txt', 'r'), '\n\n'))
for product in data:
  if not (('discontinued' in product) or product.startswith('#') or product.startswith("Total")):
    product = [key.split(':', 1) for key in product.split('\n')]
    if (product[0][0] != ''):
      create_obj(product)


product = 'B00004R99S'

def letra_a():
  print('Letra a)')
  partial_results = products.find_one({'ASIN': product}, {'_id': 0, 'reviews': 1})
  reviews = db['reviews']
  reviews.delete_many({})
  reviews.insert_many(partial_results['reviews'])
  partial_results = list(reviews.aggregate([{ '$sort': { 'helpful': -1, 'rating': -1 } }, { '$limit': 5 }, { '$project': { '_id': 0 } }]))
  print(partial_results)
  partial_results = list(reviews.aggregate([{ '$sort': { 'helpful': -1, 'rating': 1 } }, { '$limit': 5 }, { '$project': { '_id': 0 } }]))
  print(partial_results)

letra_a()

def letra_b():
  print('\nLetra b)')
  salesrank = products.find_one({'ASIN': product}, {'_id': 0, 'salesrank': 1})['salesrank']
  print(salesrank)
  b = list(products.find({'salesrank': { '$lt': salesrank },}, { '_id': 0, 'ASIN': 1, 'similar': 1, 'salesrank': 1 }))
  print(b)

letra_b()

def letra_c():
  print('\nLetra c)')
  partial_results = list(products.aggregate([{ '$match': { 'ASIN': product } },{ '$unwind': '$reviews' },{'$project': {'_id': 0,'date': '$reviews.date','rating': '$reviews.rating',}},
    {
      '$group': {
        '_id': '$date',
        'rating': { '$sum': '$rating' },
        'count': { '$sum': 1 }
      }
    },
    { '$sort': { '_id': 1 } },
    {
      '$project': {
        '_id': 0,
        'date': '$_id',
        'rating': 1,
        'count': 1,
      }
    }
  ]))
  reviews = db['reviews']
  reviews.delete_many({})
  reviews.insert_many(partial_results)
  partial_results = list(reviews.aggregate([
    {
      '$lookup': {
        'from': 'reviews',
        'let': { 'review_date': '$date' },
        'pipeline': [
          {
            '$match': {
              '$expr': {
                '$lt': [ '$date', '$$review_date' ]
              }
            }
          },
          { '$project': { '_id': 0, 'rating': 1, 'count': 1 } }
        ],
        'as': 'cumulative'
      }
    },
    {
      '$project': {
        'date': 1,
        'rating': {
          '$reduce': {
            'input': '$cumulative.rating',
            'initialValue': '$rating',
            'in': { '$add': [ '$$value', '$$this' ] }
          }
        },
        'count': {
          '$reduce': {
            'input': '$cumulative.count',
            'initialValue': '$count',
            'in': { '$add': [ '$$value', '$$this' ] }
          }
        }
      }
    },
    {
      '$project': {
        '_id': 0,
        'date': 1,
        'avg_rating': { '$divide': [ '$rating', '$count' ] }
      }
    }
  ]))
  reviews.delete_many({})
  print(partial_results)

letra_c()

def letra_e():
  print('\nLetra e)')
  # Letra E:
  partial_results = list(products.aggregate([
    {
      '$project': {
        '_id': 0,
        'ASIN': 1,
        'reviews': 1
      }
    },
    { '$unwind': '$reviews' },
    { '$match': { 'reviews.rating': { '$gte': 5 } } },
    {
      '$group': {
        '_id': '$ASIN',
        'sumHelpful': { '$sum': '$reviews.helpful' },
        'countHelpful': { '$sum': 1 }
      }
    },
    {
      '$project': {
        '_id': 0,
        'ASIN': '$_id',
        'avgHelpful': { '$divide': [ '$sumHelpful', '$countHelpful' ] }
      }
    },
    { '$sort': { 'avgHelpful': -1 } },
    { '$limit': 10 }
  ]))
  print(partial_results)

letra_e()

def letra_f():
  print('\nLetra f')
  # Letra F:
  partial_results = list(products.aggregate([
    {
      '$project': {
        '_id': 0,
        'categories': 1,
        'reviews': 1
      }
    },
    { '$unwind': '$categories' },
    { '$unwind': '$reviews' },
    { '$match': { 'reviews.rating': { '$gte': 5 } } },
    {
      '$group': {
        '_id': '$categories',
        'sumHelpful': { '$sum': '$reviews.helpful' },
        'countHelpful': { '$sum': 1 }
      }
    },
    {
      '$project': {
        '_id': 0,
        'category': '$_id',
        'avgHelpful': { '$divide': [ '$sumHelpful', '$countHelpful' ] }
      }
    },
    { '$sort': { 'avgHelpful': -1 } },
    { '$limit': 5 }
  ]))
  print(partial_results)

letra_f()