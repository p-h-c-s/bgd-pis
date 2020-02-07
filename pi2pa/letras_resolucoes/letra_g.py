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

def get_reviewers_by_group(item):
  group = re.search(r'group:\s*(\w+)', item)
  if not group:
    return []

  group = group.group(1)
  reviews = re.findall(r'cutomer:\s+[\w\d]+', item)
  return map(lambda r: (
    (group, re.search(r'cutomer:\s+([\w\d]+)', r).group(1)), 1), reviews)

groups = file.flatMap(get_reviewers_by_group)
reviewers_by_group = groups.reduceByKey(lambda x,y: x+y)
reviewers_by_group = reviewers_by_group.map(
    lambda x: (x[0][0], [(x[0][1], x[1])]))
top_reviweres_by_group = reviewers_by_group.reduceByKey(
    lambda x, y: nlargest(5, x + y, key=itemgetter(1)))
print(top_reviweres_by_group.collect())
print(top_reviweres_by_group.collect())