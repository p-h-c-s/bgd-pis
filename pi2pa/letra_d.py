import re
from heapq import nlargest
from pyspark import SparkContext, SparkConf
appName = 'bgd'

# product = input('Qual produto: \n')
product = "0827229534"
print("ASIN: {}".format(product))


conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile('./data/teste.txt')

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")



# (Group, (ASIN, salesrank))

# def extractGroupSales(file):
#   for line in file[1].split("\n"):
#     if(re.match('ASIN', line.strip())):
#       asin = line.strip('ASIN: ')
#   yield (asin)
# asins = file.flatMap(extractGroupSales)

# primeira posição na maioria das vezes é a 'chave'
#formatação
filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
asi = filtered.map(lambda s: s.splitlines())
keyValue = asi.map(lambda s: [elem.split(':') for elem in s])
stripped = keyValue.map(lambda s: [tuple([x.strip() for x in elem]) for elem in s])

pairs = stripped.map(lambda p: (p[3][1], (p[1][1], p[4][1])))
groups = pairs.groupBy(lambda x: x[0]).flatMap(lambda g: nlargest(10, g[1])).collect()
# groups = pairs.groupByKey().map(lambda g: g)
print(groups)


# encontra o maior salesrank geral
# maxa = pairs.reduceByKey(lambda x,y: x if x[1]>y[1] else y)
# print(maxa.collect())

# words = ASINs.map(lambda w: (w, 1))
# counts = words.reduceByKey(lambda a, b: a + b)
# print(filtered.collect())
