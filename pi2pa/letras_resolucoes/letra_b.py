
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


def get_sales_rank(item):
  return re.search(fr'salesrank:\s*(\d+)\n', item).group(1)

selected = file.filter(
  lambda item: re.search(fr'ASIN:\s*{product}', item))
selected_sales_rank = get_sales_rank(selected.top(1)[0])
print(selected_sales_rank)
similars = file.filter(
  lambda item: re.search(fr'similar:.+{product}.*\n', item) and int(get_sales_rank(item)) < int(selected_sales_rank))
similars_asins = similars.map(lambda item: re.search(
    fr'ASIN:\s*([\w\d]+)\n', item).group(1))
print(similars_asins.collect())