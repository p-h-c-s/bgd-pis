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

def get_formated_review(r):
  review = re.search(r'(\d+-\d+-\d+).*rating:\s+(\d+).*', r)
  return (review.group(1), (int(review.group(2)), 1))

def cumulative_sum(values):
  return [k for k in zip([i[0] for i in values], accumulate([i[1] for i in values], func=lambda a, b: (a[0]+b[0], a[1]+b[1])))]

selected = file.filter(
  lambda item: re.search(fr'ASIN:\s*{product}', item))
reviews = selected.flatMap(
  lambda item: re.findall(r'\d+-\d+-\d+\s+cutomer:[\s\w\d]+rating:[\s\d]+votes:[\s\d]+helpful:\s+\d+', item))
formated_reviews = reviews.map(get_formated_review)
ratings_by_day = formated_reviews.reduceByKey(
    lambda a, n: (a[0] + n[0], a[1] + n[1])).sortBy(lambda x: x[0].split('-')).map(lambda x: (0, x))
accumulated_data = ratings_by_day.groupByKey().mapValues(
    cumulative_sum).flatMap(lambda x: x[1]).mapValues(lambda x: int(round(x[0] / x[1])))
print(accumulated_data.collect())