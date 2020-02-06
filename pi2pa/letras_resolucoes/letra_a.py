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

def format_review(r):
    helpful = re.search(r'helpful:\s+(\d+)', r).group(1)
    rating = re.search(r'rating:\s+(\d+)', r).group(1)
    return (r, (int(helpful), int(rating)))

selected = file.filter(
    lambda item: re.search(str(f'ASIN:[ ]*{product}'), item))
reviews = selected.flatMap(
    lambda item: re.findall(r'\d+-\d+-\d+\s+cutomer:[\s\w\d]+rating:[\s\d]+votes:[\s\d]+helpful:\s+\d+', item))
reviews = reviews.map(format_review)
max_reviews = reviews.sortBy(lambda r: r[1][1], ascending=False)
sorted_max_reviews = max_reviews.takeOrdered(5, lambda r: -r[1][0])
print(sorted_max_reviews)
min_reviews = reviews.sortBy(lambda r: r[1][1])
sorted_min_reviews = min_reviews.takeOrdered(5, lambda r: -r[1][0])
print(sorted_min_reviews)