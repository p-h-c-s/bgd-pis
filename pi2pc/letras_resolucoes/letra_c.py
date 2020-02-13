import re
import functools
from heapq import nlargest
from operator import itemgetter, add
from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *

from pyspark.sql.window import Window
from pyspark.sql.functions import *


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

filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
arrays = filtered.map(lambda line: line.split('\n'))
pre_objects = arrays.map(lambda obj: [key.split(':', 1) for key in obj])
products = pre_objects.map(create_obj)

df = products.toDF(schema)

product = '0486220125'

sum_udf = udf(lambda x: functools.reduce(add, x), IntegerType())

reviews = df.filter(df.ASIN == product).select(explode(df.reviews).alias('reviews'))
reviews = reviews.withColumn('customer', reviews.reviews.customer)
reviews = reviews.withColumn('helpful', reviews.reviews.helpful.cast(IntegerType()))
reviews = reviews.withColumn('rating', reviews.reviews.rating.cast(IntegerType()))
reviews = reviews.withColumn('date', reviews.reviews.date)
reviews = reviews.withColumn('votes', reviews.reviews.votes.cast(IntegerType()))
df = reviews
df = df.select(df.date, df.rating).withColumn('quant', lit(1)).orderBy(df.date)
df = df.withColumn('cumulativeRating', sum_udf(collect_list(df.rating).over(Window.orderBy('date'))))
df = df.withColumn('cumulativeQuant', sum_udf(collect_list(df.quant).over(Window.orderBy('date'))))
df = df.withColumn('avgRating', (df.cumulativeRating / df.cumulativeQuant).cast(IntegerType()))
df = df.select(df.date, df.avgRating)
df.show(len(df.collect()))