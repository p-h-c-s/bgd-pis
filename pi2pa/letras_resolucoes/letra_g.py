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

def customPrint(data):
  for i in data:
    print('Clientes que mais votaram para a categoria {}'.format(i[0]))
    for client in i[1]:
      print(client)

def getReviews(stripped):
  allReviews = []
  for i in range(len(stripped)):
    if stripped[i].startswith('reviews'):
      for j in range(i+1, len(stripped)):
        allReviews.append(stripped[j])
      return allReviews

def get_reviewers_by_group(item):
  group = item[3].split('group:')[1]
  if not group:
    return []
  reviews = getReviews(item)
  for r in reviews:
    yield ((group, r.split('cutomer:')[1].split('rating')[0]), 1)

filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
asi = filtered.map(lambda s: s.rstrip().split('\n'))
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])


groups = stripped.flatMap(get_reviewers_by_group)
reviewers_by_group = groups.reduceByKey(lambda x,y: x+y)
reviewers_by_group = reviewers_by_group.map(
    lambda x: (x[0][0], [(x[0][1], x[1])]))
top_reviweres_by_group = reviewers_by_group.reduceByKey(
    lambda x, y: nlargest(10, x + y, key=itemgetter(1)))
customPrint(top_reviweres_by_group.collect())