import re
import functools
from heapq import nlargest
from operator import itemgetter, add
from pyspark import SparkContext, SparkConf

from pyspark.sql import SparkSession, functions
from pyspark.sql.types import *

from pyspark.sql.window import Window
from pyspark.sql.functions import *

import sys


appName = 'bgd'

path_to_file = sys.argv[1]

sc = SparkContext()
sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")
spark = SparkSession.builder.appName('bgd').getOrCreate()
file = sc.textFile(path_to_file)

# product = input('insira o ASIN do produto a ser buscado')
product = 1558608915

#Funções auxiliares:

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

# Parser inicial:

filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
arrays = filtered.map(lambda line: line.split('\n'))
pre_objects = arrays.map(lambda obj: [key.split(':', 1) for key in obj])
products = pre_objects.map(create_obj)

# Letra a):
print('\nLETRA A): \n')

dfA = products.toDF(schema)

print('Resultado utilizando dataframes')
reviews = dfA.filter(dfA.ASIN == product).select(explode(dfA.reviews).alias('reviews'))
reviews = reviews.withColumn('customer', reviews.reviews.customer)
reviews = reviews.withColumn('helpful', reviews.reviews.helpful)
reviews = reviews.withColumn('rating', reviews.reviews.rating)
reviews = reviews.withColumn('date', reviews.reviews.date)
reviews = reviews.withColumn('votes', reviews.reviews.votes)
print('\n5 comentários mais úteis e com maior avaliação: ')
reviews.orderBy(reviews.helpful.desc(), reviews.rating.desc()).limit(5).show()
print('\n5 comentários mais úteis com menor avaliação: ')
reviews.orderBy(reviews.helpful.desc(), reviews.rating.asc()).limit(5).show()

# Como view
print('\n\n-----------------\n Utilizando views: 5 comentários mais úteis e com maior avaliação')
products.toDF(schema).createOrReplaceTempView("products")
spark.sql(""" 
  SELECT * FROM (
    SELECT new_reviews.date, new_reviews.customer, new_reviews.helpful, new_reviews.rating
    FROM products
    LATERAL VIEW explode(reviews) exploded_reviews AS new_reviews
    WHERE ASIN='{}'
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
    WHERE ASIN='{}'
  ) ORDER BY helpful DESC, rating ASC LIMIT 5
""".format(product)
).show(n=50)

# ----------------------------------------
print('\n')
# Letra b)
print('LETRA B): \n')
product = 'B00004R99S'

print('Com dataframes')
dfB = products.toDF(schema)
selectedProductSalesRank = dfB.filter(dfB.ASIN == product).collect()[0].salesrank
similars = dfB.select(dfB.ASIN, dfB.salesrank, explode(dfB.similar).alias('similar'))
similars = similars.filter(similars.salesrank > selectedProductSalesRank).where(similars.similar == product)
similars.show(n=50)

# Como view
print('\n\n-----------------\n Produtos similares com salesrank maior que {}'.format(product))
products.toDF(schema).createOrReplaceTempView("products")
spark.sql(""" 
    SELECT  similars.ASIN, similars.salesrank, ex_similars, products.salesrank FROM
    (
        SELECT products.ASIN, salesrank, ex_similars
        FROM products
        LATERAL VIEW explode(similar) exploded_similars AS ex_similars
        WHERE products.ASIN='{}'
    ) as similars JOIN products ON products.ASIN=similars.ex_similars
    WHERE similars.salesrank < products.salesrank
""".format(product)
).show(n=50)

# ----------------------------------------
print('\n')
# Letra c)
print('LETRA C): \n')

product = '0486220125'
dfC = products.toDF(schema)

sum_udf = udf(lambda x: functools.reduce(add, x), IntegerType())
print('\n\n-----------------\n Evolução das avaliações de {}'.format(product))
reviews = dfC.filter(dfC.ASIN == product).select(explode(dfC.reviews).alias('reviews'))
reviews = reviews.withColumn('customer', reviews.reviews.customer)
reviews = reviews.withColumn('helpful', reviews.reviews.helpful.cast(IntegerType()))
reviews = reviews.withColumn('rating', reviews.reviews.rating.cast(IntegerType()))
reviews = reviews.withColumn('date', reviews.reviews.date)
reviews = reviews.withColumn('votes', reviews.reviews.votes.cast(IntegerType()))
dfC = reviews
dfC = dfC.select(dfC.date, dfC.rating).withColumn('quant', lit(1)).orderBy(dfC.date)
dfC = dfC.withColumn('cumulativeRating', sum_udf(collect_list(dfC.rating).over(Window.orderBy('date'))))
dfC = dfC.withColumn('cumulativeQuant', sum_udf(collect_list(dfC.quant).over(Window.orderBy('date'))))
dfC = dfC.withColumn('avgRating', (dfC.cumulativeRating / dfC.cumulativeQuant).cast(IntegerType()))
dfC = dfC.select(dfC.date, dfC.avgRating)
dfC.show(len(dfC.collect()))

# Como view
print('\n Utilizando views')
print('\n\n-----------------\n Evolução das avaliações de {}'.format(product))
products.toDF(schema).createOrReplaceTempView("products")
spark.sql(""" 
SELECT date, 
        AVG(rating) OVER(ORDER BY date ROWS BETWEEN unbounded preceding AND CURRENT ROW) rating
    FROM (
        SELECT exploded_reviews.date, exploded_reviews.rating
        FROM products
        LATERAL VIEW explode(reviews) exploded_reviews AS exploded_reviews
        WHERE ASIN='{}'
    )
""".format(product)
).show(n=50)


# ----------------------------------------
# print('\n')
# # Letra d)
print('LETRA D): \n')

dfD = products.toDF(schema).where('salesrank >= 0')

window = Window.partitionBy(dfD['group']).orderBy(['salesrank', 'ASIN'])

dfD.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 10).show(n=100)

# Como view
print('\n\n-----------------\n Utilizando views:')
products.toDF(schema).createOrReplaceTempView("products")
spark.sql(""" 
SELECT * 
    FROM (
        SELECT *, rank() OVER (PARTITION BY products.group ORDER BY products.salesrank ASC, products.ASIN ASC) as rank 
    FROM PRODUCTS WHERE salesrank >= 0) jobs 
    WHERE 
        rank <= 10
"""
).show(n=100)


# ----------------------------------------
# print('\n')
# # Letra f)
print('LETRA F): \n')

def calcHelpful(reviews):
  sumResult = 0
  if len(reviews) > 0:
    for r in reviews:
      if(int(r['rating']) >= 5):
        sumResult += int(r['helpful'])
  return sumResult

def calcRating(reviews):
  count = 1
  if len(reviews) > 0:
    for r in reviews:
      if(int(r['rating']) >= 5):
        count += 1
  return count

df = products.toDF(schema)

calcSum = udf(calcHelpful, IntegerType())
calcQuant = udf(calcRating, IntegerType())

print('Com dataframes')
df = df.select(df.group, df.reviews)
df = df.withColumn('sum', calcSum(df.reviews)).withColumn('quant', calcQuant(df.reviews)).select(df.group, 'sum', 'quant')
df = df.groupBy(df.group).agg({'sum': 'sum', 'quant': 'sum'})
df = df.withColumn('avgRating', df['sum(sum)'] / df['sum(quant)']).select(df.group, 'avgRating')
df = df.orderBy(df.avgRating.desc()).limit(5)
df.show(len(df.collect()))


print('\n\nCom views')
products.toDF(schema).createOrReplaceTempView("products")
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
# # ----------------------------------------
# print('\n')
# # Letra g)
print('LETRA G): \n')
print('Com dataframes:')
df = products.toDF(schema)

custudf = udf(lambda x: [b['customer'] for b in x], ArrayType(StringType()))

new_df = df.withColumn('customer', custudf(df.reviews))
exploded = new_df.withColumn('customer', explode(new_df.customer))

df_count = exploded.groupBy('group', 'customer').agg(count('customer').alias('frequencia'))
# print(df_count.show(n=50))
window = Window.partitionBy('group').orderBy(desc('frequencia'), 'customer')

df_final = df_count.select('*', rank().over(window).alias('rank')).filter(col('rank') <= 10)

print(df_final.show(n=50))


# Com views:
print('\n\n-----------------\n Utilizando views:')
products.toDF(schema).createOrReplaceTempView("products")
spark.sql(""" 
SELECT * 
FROM (
    SELECT *, rank() OVER (PARTITION BY group ORDER BY freq DESC) as rank 
    FROM (
        SELECT group, customer, count(customer) as freq 
        FROM (
            SELECT group, new_reviews.customer
            FROM products
            LATERAL VIEW explode(reviews) exploded_reviews AS new_reviews
) GROUP BY group,customer ORDER BY count(customer)))
WHERE rank <= 10 
"""
).show(n=200)