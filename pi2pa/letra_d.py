import re
from heapq import nlargest
from pyspark import SparkContext, SparkConf
appName = 'bgd'

conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile('./data/teste.txt')

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")


# primeira posição na maioria das vezes é a 'chave'
#formatação
filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
asi = filtered.map(lambda s: s.splitlines())
keyValue = asi.map(lambda s: [elem.split(':') for elem in s])
stripped = keyValue.map(lambda s: [tuple([x.strip() for x in elem]) for elem in s])

pairs = stripped.map(lambda p: (p[3][1], (p[1][1], p[4][1])))
groups = pairs.groupByKey().flatMap(lambda g: (g[0] ,nlargest(10, g[1])))
print(groups.count())
