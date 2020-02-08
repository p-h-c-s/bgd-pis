import re
from heapq import nsmallest,nlargest
from operator import itemgetter
from itertools import accumulate
from pyspark import SparkContext, SparkConf
appName = 'bgd'

path_to_file = input('insira o path para o arquivo de entrada: ')

conf = SparkConf().setAppName(appName).setMaster('local')
conf.set("spark.executor.memory","2G")
sc = SparkContext(conf=conf)
file = sc.textFile(path_to_file)

sc._jsc.hadoopConfiguration().set("textinputformat.record.delimiter", "\n\n")

# product = input('insira o ASIN do produto a ser buscado')
product = 1561893684

# Letra a):
print('\nLETRA A): \n')

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


# ----------------------------------------
print('\n')
# Letra b)
print('LETRA B): \n')

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

# ----------------------------------------
print('\n')
# Letra c)
print('LETRA C): \n')

def recurrent_values(values):
  return [k for k in zip([i[0] for i in values], accumulate([i[1] for i in values], func=lambda a, b: (a[0]+b[0], a[1]+b[1])))]

def getReviewsC(stripped):
  allReviews = []
  for i in range(len(stripped)):
    if stripped[i].startswith('reviews'):
      for j in range(i+1, len(stripped)):
        allReviews.append(stripped[j])
      return allReviews

def formatC(r):
    return (r.split('rating')[0], (int(r.split('rating:')[1][0]), 1))        

selected = file.filter(
  lambda item: re.search(str(f'ASIN:[ ]*{product}'), item))

asi = selected.map(lambda s: s.rstrip().split('\n'))
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])
review = stripped.flatMap(getReviewsC)
formated_reviews = review.map(formatC)
  
ratings_timeline = formated_reviews.reduceByKey(
    lambda a, n: (a[0] + n[0], a[1] + n[1])).sortBy(lambda x: x[0].split('-')).map(lambda x: (0, x))
all_data = ratings_timeline.groupByKey().mapValues(
    recurrent_values).flatMap(lambda x: x[1]).mapValues(lambda x: int(round(x[0] / x[1])))
print('Evolução diária das médias de avaliação do produto {}'.format(product))
print(list(map(lambda x: (x[0].split('cutomer')[0], x[1]), all_data.collect())))

# ----------------------------------------
print('\n')
# Letra d)
print('LETRA D): \n')

def customPrintD(data):
  for i in data:
    print('Categoria: {}'.format(i[0]))
    for client in i[1]:
      print(client)

#formatação
filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
asi = filtered.map(lambda s: s.split('\n')[:-1])
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])

pairsFiltered = stripped.filter(lambda p: int(p[4][10:]) != -1)
pairs = pairsFiltered.map(lambda p: (p[3][6:], (p[1][5:], int(p[4][10:]))))
# o itemgetter(0) escolhe qual campo comparar
groups = pairs.groupByKey().map(lambda g: (g[0] ,nsmallest(10, g[1], key = itemgetter(1))))
print('10 produtos líderes de vendas em cada grupo')
customPrintD(groups.collect())

# ----------------------------------------
print('\n')
# Letra e)
print('LETRA E): \n')

def getReviewsE(stripped):
  allReviews = []
  for i in range(len(stripped)):
    if stripped[i].startswith('reviews'):
      for j in range(i+1, len(stripped)):
        allReviews.append(stripped[j])
      return allReviews

def calcAverage(reviews):
  avgRating = 0
  count = 1
  for r in reviews:
    if(int(r.split('rating:')[1][0]) >= 5):
      count += 1
      if(len(r.split('helpful:')) > 1):
        avgRating += int(r.split('helpful:')[1])

  return avgRating/count

filtered = file.filter(lambda l: not (('  discontinued' in l) or l.startswith('#') or l.startswith("Total")))
asi = filtered.map(lambda s: s.rstrip().split('\n'))
stripped = asi.map(lambda s: [elem.replace(" ","") for elem in s])

pairs = stripped.map(lambda p: (p[1].split('ASIN:')[1], calcAverage(getReviewsE(p[6:]))))
groups = pairs.top(10, key=lambda x: x[1])
print('10 produtos com maior média de avaliações')
print(groups)

# ----------------------------------------
print('\n')
# Letra f)
print('LETRA F): \n')

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
print('5 categorias de produto com a maior média de avaliações úteis positivas por produto')
print(list(map(lambda x: (x[0].split('|')[-1], x[1]), best_categories)))


# ----------------------------------------
print('\n')
# Letra g)
print('LETRA G): \n')

def customPrintG(data):
  for i in data:
    print('Clientes que mais votaram para a categoria {}'.format(i[0]))
    for client in i[1]:
      print(client)

def getReviewsG(stripped):
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
  reviews = getReviewsG(item)
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
customPrintG(top_reviweres_by_group.collect())