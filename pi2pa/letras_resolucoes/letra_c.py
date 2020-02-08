import re
from itertools import accumulate
from pyspark import SparkContext, SparkConf
appName = 'bgd'

conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile('./data/amazon-meta.txt')

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

# product = input('insira o ASIN do produto a ser buscado')
product = 1561893684

def recurrent_values(values):
  return [k for k in zip([i[0] for i in values], accumulate([i[1] for i in values], func=lambda a, b: (a[0]+b[0], a[1]+b[1])))]

def getReviews(stripped):
  allReviews = []
  for i in range(len(stripped)):
    if stripped[i].startswith('reviews'):
      for j in range(i+1, len(stripped)):
        allReviews.append(stripped[j])
      return allReviews

def format(r):
    return (r.split('rating')[0], (int(r.split('rating:')[1][0]), 1))        

selected = file.filter(
  lambda item: re.search(str(f'ASIN:[ ]*{product}'), item))

asi = selected.map(lambda s: s.rstrip().split('\n'))
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])
review = stripped.flatMap(getReviews)
formated_reviews = review.map(format)
  
ratings_timeline = formated_reviews.reduceByKey(
    lambda a, n: (a[0] + n[0], a[1] + n[1])).sortBy(lambda x: x[0].split('-')).map(lambda x: (0, x))
all_data = ratings_timeline.groupByKey().mapValues(
    recurrent_values).flatMap(lambda x: x[1]).mapValues(lambda x: int(round(x[0] / x[1])))
print(list(map(lambda x: (x[0].split('cutomer')[0], x[1]), all_data.collect())))