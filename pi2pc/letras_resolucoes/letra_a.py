import re
from heapq import nlargest
from operator import itemgetter
from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *

from pyspark.sql.window import Window
from pyspark.sql.functions import rank, col, udf, explode, count, desc, asc, posexplode


appName = 'bgd'

conf = SparkConf().setAppName(appName)
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

spark = SparkSession.builder.appName('bgd').getOrCreate()

file = sc.textFile('../pi2pa/data/amazon-meta.txt')

# positiva e com rating = 5
# pegar cada produto, calcular a media de review e pegar com 10 maiores na heapq

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

# raw dataframe api (nosql)
# https://spark.apache.org/docs/2.1.0/api/python/pyspark.sql.html#pyspark.sql.DataFrame

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

product = 1561893684

filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
arrays = filtered.map(lambda line: line.split('\n'))
pre_objects = arrays.map(lambda obj: [key.split(':', 1) for key in obj])
products = pre_objects.map(create_obj)

df = products.toDF(schema)

schemaRaw = [('Id', IntegerType()),
            ('ASIN', StringType()),
            ('title', StringType()),
            ('group', StringType()), 
            ('salesrank', IntegerType()), 
            ('similar', ArrayType(StringType())), 
            ('categories', ArrayType(StringType())),
            ('reviews', ArrayType(MapType(StringType(), StringType())))]
fields = [StructField(field[0], field[1], True) for field in schemaRaw]
schema = StructType(fields)

df = products.toDF(schema)

print('Resultado utilizando dataframes')
product = '0738700797'
reviews = df.filter(df.ASIN == product).select(explode(df.reviews).alias('reviews'))
reviews = reviews.withColumn('customer', reviews.reviews.customer)
reviews = reviews.withColumn('helpful', reviews.reviews.helpful)
reviews = reviews.withColumn('rating', reviews.reviews.rating)
reviews = reviews.withColumn('date', reviews.reviews.date)
reviews = reviews.withColumn('votes', reviews.reviews.votes)
reviews.orderBy(reviews.helpful.desc(), reviews.rating.desc()).limit(5).show()
reviews.orderBy(reviews.helpful.desc(), reviews.rating.asc()).limit(5).show()

# Como view
print('\n\n-----------------\n Utilizando views: 5 comentários mais úteis e com maior avaliação')
products.toDF(schema).createOrReplaceTempView("products")
spark.sql(""" 
  SELECT * FROM (
    SELECT new_reviews.date, new_reviews.customer, new_reviews.helpful, new_reviews.rating
    FROM products
    LATERAL VIEW explode(reviews) exploded_reviews AS new_reviews
    WHERE ASIN={}
  ) ORDER BY helpful DESC, rating DESC LIMIT 5
""".format(product)
).show(n=50)

print('\n\n-----------------\n Segunda query: 5 comentários mais úteis e com menor avaliação')
products.toDF(schema).createOrReplaceTempView("products")
spark.sql(""" 
  SELECT * FROM (
    SELECT new_reviews.date, new_reviews.customer, new_reviews.helpful, new_reviews.rating
    FROM products
    LATERAL VIEW explode(reviews) exploded_reviews AS new_reviews
    WHERE ASIN={}
  ) ORDER BY helpful DESC, rating ASC LIMIT 5
""".format(product)
).show(n=50)