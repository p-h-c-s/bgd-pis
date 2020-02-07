import re
from heapq import nsmallest
from operator import itemgetter
from pyspark import SparkContext, SparkConf
appName = 'bgd'

conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile('./data/amazon-meta.txt')

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

# pode ter ':' dentro do titulo -> cuidado com o parse
# algumas linhas tem newline do unicode. o splitlines do python pega, nao quero
# primeira posição na maioria das vezes é a 'chave'
#formatação
filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
asi = filtered.map(lambda s: s.split('\n')[:-1])
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])

pairs = stripped.map(lambda p: (p[3][6:], (p[1][5:], int(p[4][10:]))))
# o itemgetter(0) escolhe qual campo comparar
groups = pairs.groupByKey().flatMap(lambda g: (g[0] ,nsmallest(10, g[1], key = itemgetter(1))))
print(groups.collect())