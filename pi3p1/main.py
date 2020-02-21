from sklearn.datasets import fetch_20newsgroups
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, Word2Vec, MinHashLSH
from operator import itemgetter
import math
import builtins

conf = SparkConf()
conf.set('spark.logConf', 'true')
spark = SparkSession.builder.config(conf=conf).appName('bgd').getOrCreate()
spark.sparkContext.setLogLevel("OFF")

proc_data = fetch_20newsgroups(subset='all', remove=('headers', 'footers', 'quotes'))

test_data = fetch_20newsgroups(subset='test', remove=('headers', 'footers', 'quotes'))

TOTAL_AMOUNT = len(proc_data.target)
# TEST_AMOUNT = TOTAL_AMOUNT/3
TEST_AMOUNT = 10

# k parameter for A-NN
K_VALUE = 5

# parameter for minhashlsh
NUM_HASH_TABLES = 5

raw_groups = list(zip(proc_data.data, proc_data.target.tolist()))
raw_groups[:] = [x for x in raw_groups if (len(x[0].replace('\n', '')) > 0)]

def vectorizeDF(raw):
  raw = spark.createDataFrame(raw_groups, schema=['data', 'target'])
  raw = raw.withColumn('id', monotonically_increasing_id())

  tokenizer = Tokenizer(inputCol='data', outputCol='tokens')
  tok_data = tokenizer.transform(raw)

  swremover = StopWordsRemover(inputCol='tokens', outputCol='words')
  rm_data = swremover.transform(tok_data)

  cv = CountVectorizer(inputCol='words', outputCol='features', vocabSize=1000)
  cvmodel = cv.fit(rm_data)
  feat_data = cvmodel.transform(rm_data)
  checkZero = udf(lambda V: V.numNonzeros() > 0, BooleanType())

  feat_data = feat_data.filter(checkZero(col('features')))
  return feat_data

# create dataframe from raw data
train = vectorizeDF(proc_data)

# Create LSH model
mh = MinHashLSH(inputCol='features', outputCol='hashes', numHashTables=NUM_HASH_TABLES, seed = 5123)
model = mh.fit(train)
train = model.transform(train)

extractExploded = udf(lambda l: float(l[0]), FloatType())
train = train.select('id', 'features', 'target', explode('hashes').alias('hashes'))
train = train.withColumn('extracted', extractExploded(col('hashes')))

# split data in the ratio: 5 train to 1 test
train,teste = train.randomSplit([5.0, 1.0], 24)

# helper function to extract values from a ROW
def extractValues(test_value):
  values = {}
  values['id'] = test_value['id']
  values['target'] = test_value['target']
  values['features'] = test_value['features']
  values['extracted'] = test_value['extracted']
  return values

test_values = teste.take(TEST_AMOUNT)

# ver se tem pelo menos um hash igual. se tiver, usar a keyDist
# vect2 por exemplo e o vetor de teste
def keyDist(vect1, vect2):
  set1 = set(vect1)
  set2 = set(vect2)
  intersectSize = len(set1.intersection(set2))
  union = float(len(set1) + len(set2) - intersectSize)
  return 1 - intersectSize/union

def distAux(keyFeat):
  return udf(lambda l: keyDist(l, keyFeat), DoubleType())


def A_NN(test_values, train_dataframe):
  true_positives = 0
  false_negatives = 0
  for value in test_values:
    value = extractValues(value)

    # assumindo que o spark sabe comparar floats de maneira decente
    hashEncounters = train_dataframe.where('extracted == {}'.format(value['extracted']))

    print('A-NN para a entrada de teste de id: {} com target: {}'.format(value['id'], value['target']))
    dists = hashEncounters.withColumn('distCol', distAux(value['features'])(col('features')))
    topK = dists.orderBy(col('distCol').desc()).limit(K_VALUE)
    predictedList = topK.select('target').collect()
    votes = {}
    for target in predictedList[0]:
      if(target not in votes):
        votes[target] = 1
      else:
        votes[target] += 1

    # max builtin was overwritten by a pyspark module, too lazy to find which
    selectedTarget = builtins.max(votes.items(), key = itemgetter(1))[0]
    print('Classificação: {}'.format(selectedTarget))
    if(selectedTarget == value['target']):
      print('Hit')
      true_positives += 1
    else:
      print('Miss')
      false_negatives += 1
  # precision = true_positives/(true_positives+false_positives)
  # recall = true_positives/(true_positives)
  print(true_positives/(false_negatives+true_positives))

A_NN(test_values,train)