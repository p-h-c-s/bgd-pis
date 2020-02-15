import re
from heapq import nlargest
from operator import itemgetter
from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, udf, explode, count, desc


appName = 'bgd'

conf = SparkConf().setAppName(appName)
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

spark = SparkSession.builder.appName('bgd').getOrCreate()

file = sc.textFile('../pi2pa/data/amazon-meta.txt')

def calcSumHelpful(reviews):
  sumResult = 0
  if len(reviews) > 0:
    for r in reviews:
      if(int(r['rating']) >= 5):
        sumResult += int(r['helpful'])
  return sumResult

def calcQuantRating(reviews):
  count = 1
  if len(reviews) > 0:
    for r in reviews:
      if(int(r['rating']) >= 5):
        count += 1
  return count

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
    for i in range(skip, (len(array))):
        # print(array[skip+1][0].strip().split()[0])
        obj['reviews'].append({
            'date': array[i][0].strip().split()[0],
            'customer': array[i][1].strip().split()[0],
            'rating': int(array[i][1].strip().split()[2]),
            'votes': int(array[i][1].strip().split()[4]),
            'helpful': int(array[i][1].strip().split()[6])
        })

    return obj

schemaRaw = [('Id', IntegerType()),
            ('ASIN', StringType()),
            ('title', StringType()),
            ('group', StringType()), 
            ('salesrank', IntegerType()), 
            ('similar', ArrayType(StringType())), 
            ('categories', ArrayType(StringType())),
            ('reviews', ArrayType(
                            MapType(StringType(), StringType())
                                        ))]


fields = [StructField(field[0], field[1], True) for field in schemaRaw]
schema = StructType(fields)

# def getCustomer(reviews):

filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
arrays = filtered.map(lambda line: line.split('\n'))
pre_objects = arrays.map(lambda obj: [key.split(':', 1) for key in obj])
products = pre_objects.map(create_obj)

df = products.toDF(schema)

calcSum = udf(calcSumHelpful, IntegerType())
calcQuant = udf(calcQuantRating, IntegerType())

# print('Com dataframes')
# df = df.select(df.group, df.reviews)
# df = df.withColumn('sum', calcSum(df.reviews)).withColumn('quant', calcQuant(df.reviews)).select(df.group, 'sum', 'quant')
# df = df.groupBy(df.group).agg({'sum': 'sum', 'quant': 'sum'})
# df = df.withColumn('avgRating', df['sum(sum)'] / df['sum(quant)']).select(df.group, 'avgRating')
# df = df.orderBy(df.avgRating.desc()).limit(5)
# df.show(len(df.collect()))

print('\n\nCom views')
df.createOrReplaceTempView("products")
spark.sql("""
  SELECT category, AVG(helpful) FROM(
    SELECT category, helpful FROM (
      SELECT categories, new_reviews.helpful
      FROM products
      LATERAL VIEW explode(reviews) exploded_reviews AS new_reviews
      WHERE new_reviews.rating >= 5
    )
    LATERAL VIEW explode(categories) exploded_categories AS category
  ) GROUP BY (category) ORDER BY AVG(helpful) DESC LIMIT 5
""").show()