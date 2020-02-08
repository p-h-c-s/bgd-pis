import re
from heapq import nlargest
from operator import itemgetter
from pyspark import SparkContext, SparkConf
appName = 'bgd'

conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile('./data/teste.txt')

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

def calc_average(reviews):
  avgRating = 0
  count = 1
  for r in reviews:
    rating = int(r.replace(" ", "").split('rating:')[1][0])
    if(rating >= 5):
      count += 1
      avgRating += int(r.replace(" ", "").split('helpful:')[1][0])
  return (avgRating, count)


# asins = file.filter(lambda item: re.search(r'ASIN:\s*([\w\d]+)', item))
asins = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))


reviews_by_cat = asins.map(lambda item: (re.findall(
    r'(\|.+\[\d+\])+\n', item), re.findall(r'\d+-\d+-\d+\s+cutomer:[\s\w\d]+rating:[\s\d]+votes:[\s\d]+helpful:\s+\d+', item)))
all_ratings = reviews_by_cat.mapValues(calc_average)
ratings_categories = all_ratings.flatMap (lambda item: [(category, item[1]) for category in item[0]])
ratings_by_cetegories = ratings_categories.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
mean_by_category = ratings_by_cetegories.mapValues(lambda v: v[0] / v[1])
best_categories = mean_by_category.top(5, key=lambda x: x[1])

# formatação da saída
print(list(map(lambda x: (x[0].split('|')[-1], x[1]), best_categories)))
