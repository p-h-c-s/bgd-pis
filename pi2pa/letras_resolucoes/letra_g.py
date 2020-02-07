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
  # print(item)
  group = re.search(r'group:\s*(\w+)', item)
  if not group:
    return []

  group = group.group(1)
  reviews = re.findall(
      r'\d+-\d+-\d+\s+cutomer:[\s\w\d]+rating:[\s\d]+votes:[\s\d]+helpful:\s+\d+', item)
  reviewers_by_group = map(lambda r: (
      (group, re.search(r'cutomer:\s+([\w\d]+)', r).group(1)), 1), reviews)
  return reviewers_by_group

groups = file.flatMap(get_reviewers_by_group)
reviewers_by_group = groups.reduceByKey(lambda a,b: a+b)
reviewers_by_group = reviewers_by_group.map(lambda r: (r[0][0], [(r[0][1], r[1])]))
reviewers_by_group = reviewers_by_group.reduceByKey(lambda x, y: x + y)
top_reviweres_by_group = reviewers_by_group.map(
    lambda g: (g[0] ,nlargest(10, g[1], key = itemgetter(1))))
  
# print(reviewers_by_group.collect())
print(top_reviweres_by_group.collect())