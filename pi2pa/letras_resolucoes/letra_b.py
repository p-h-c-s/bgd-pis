
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


def get_sales_rank_raw(item):
  item = item.split('\n')[:-1]
  item = [elem.replace(" ","") for elem in item]
  return item[4].split('salesrank:')[1]

def get_sales_rank(item):
  return item[4]

selected = file.filter(
  lambda item: re.search(str(f'ASIN:[ ]*{product}'), item))

asi = selected.map(lambda s: s.split('\n')[:-1])
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])

selected_sales_rank = get_sales_rank(stripped.take(1)[0]).split('salesrank:')[1]
similars = file.filter(
  lambda item: re.search(fr'similar:.+{product}.*\n', item) and int(get_sales_rank_raw(item)) < int(selected_sales_rank))
similars_asins = similars.map(lambda item: re.search(
    fr'ASIN:\s*([\w\d]+)\n', item).group(1))

print('Produtos similares com maiores vendas que {}'.format(product))
print(similars_asins.collect())