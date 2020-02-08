import re
from pyspark import SparkContext, SparkConf
appName = 'bgd'

conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile('./data/amazon-meta.txt')

# product = input('insira o ASIN do produto a ser buscado')
product = 1561893684

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

def customPrint(arr):
    for el in arr:
        print('review: {} | com rating {} e helpful {}'.format(el[0], el[1][0], el[1][1]))

def getReviews(stripped):
  allReviews = []
  for i in range(len(stripped)):
    if stripped[i].startswith('reviews'):
      for j in range(i+1, len(stripped)):
        allReviews.append(stripped[j])
      return allReviews

def format(r):
    return (r.split('rating')[0], (int(r.split('rating:')[1][0]), int(r.split('rating:')[1][0])))        

selected = file.filter(
    lambda item: re.search(str(f'ASIN:[ ]*{product}'), item))

asi = selected.map(lambda s: s.rstrip().split('\n'))
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])
review = stripped.flatMap(getReviews)
reviews = review.map(format)

print('\n5 comentários mais úteis e com maior avaliação: ')
max_reviews = reviews.sortBy(lambda r: r[1][1], ascending=False)
sorted_max_reviews = max_reviews.takeOrdered(5, lambda r: -r[1][0])
customPrint(sorted_max_reviews)

print('\n5 comentários mais úteis com menor avaliação: ')
min_reviews = reviews.sortBy(lambda r: r[1][1])
sorted_min_reviews = min_reviews.takeOrdered(5, lambda r: -r[1][0])
customPrint(sorted_min_reviews)