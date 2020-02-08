import re
from heapq import nlargest
from operator import itemgetter
from pyspark import SparkContext, SparkConf
appName = 'bgd'

conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile('./data/amazon-meta.txt')

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

# positiva e com rating = 5
# pegar cada produto, calcular a media de review e pegar com 10 maiores na heapq

def getReviews(stripped):
  allReviews = []
  for i in range(len(stripped)):
    if stripped[i].startswith('reviews'):
      for j in range(i+1, len(stripped)):
        allReviews.append(stripped[j])
      return allReviews

def calcAverage(reviews):
  avgRating = 0
  count = 1
  for r in reviews:
    if(int(r.split('rating:')[1][0]) >= 5):
      count += 1
      if(len(r.split('helpful:')) > 1):
        avgRating += int(r.split('helpful:')[1])

  return avgRating/count

filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
asi = filtered.map(lambda s: s.rstrip().split('\n'))
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])

pairs = stripped.map(lambda p: (p[1].split('ASIN:')[1], calcAverage(getReviews(p[6:]))))
groups = pairs.top(10, key=lambda x: x[1])

print(groups)