from sklearn.datasets import fetch_20newsgroups
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, Word2Vec, MinHashLSH
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from operator import itemgetter
import math
import builtins

conf = SparkConf()
conf.set('spark.logConf', 'true')
spark = SparkSession.builder.config(conf=conf).appName('bgd').getOrCreate()
spark.sparkContext.setLogLevel("OFF")

proc_data = fetch_20newsgroups(subset='all', remove=('headers', 'footers', 'quotes'))

TOTAL_AMOUNT = len(proc_data.target)

# amount of test values that are actually used. Low value for quick testing!!!
TEST_AMOUNT = 10

# k parameter for A-NN
K_VALUE = 5

# parameter for minhashlsh
NUM_HASH_TABLES = 5

raw_groups = list(zip(proc_data.data, proc_data.target.tolist()))
raw_groups[:] = [x for x in raw_groups if (len(x[0].replace('\n', '')) > 0)]

# Pipeline function to perform all transformations
def vectorizeDF(raw):
  raw = spark.createDataFrame(raw_groups, schema=['data', 'target'])
  raw = raw.withColumn('id', monotonically_increasing_id())

  tokenizer = Tokenizer(inputCol='data', outputCol='tokens')

  swremover = StopWordsRemover(inputCol='tokens', outputCol='words')

  cv = CountVectorizer(inputCol='words', outputCol='features', vocabSize=100)

  mh = MinHashLSH(inputCol='features', outputCol='hashes', numHashTables=NUM_HASH_TABLES, seed = 5123)

  pipeline = Pipeline(stages=[tokenizer,swremover,cv, mh])
  feat_data = pipeline.fit(dataset=raw).transform(raw)
  checkZero = udf(lambda V: V.numNonzeros() > 0, BooleanType())

  feat_data = feat_data.filter(checkZero(col('features')))
  return feat_data

# apply entire pipeline to raw_data
train = vectorizeDF(proc_data)

# extract hashes from denseVectors
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

# take only a few values for quick testing.
test_values = teste.take(TEST_AMOUNT)

# Jaccard distance for two sparse vectors
def keyDist(vect1, vect2):
  set1 = set(vect1)
  set2 = set(vect2)
  intersectSize = len(set1.intersection(set2))
  union = float(len(set1) + len(set2) - intersectSize)
  return 1 - intersectSize/union

# Helper function for jaccard distance
def distAux(keyFeat):
  return udf(lambda l: keyDist(l, keyFeat), DoubleType())

""" Main A-NN function.
  The basic idea is using the hashes from minhashLSH to find candidate pairs. 
  Then we measure the distance between those candidate pairs with jaccard distance. 
  The 'k' closest values then 'vote' to classify the test entry.
"""
def A_NN(test_values, train_dataframe):
  predictionsAndLabels = []
  for value in test_values:
    value = extractValues(value)

    hashEncounters = train_dataframe.where('extracted == {}'.format(value['extracted']))
    print('A-NN para a entrada de teste de id: {} com target: {}'.format(value['id'], value['target']))
    dists = hashEncounters.withColumn('distCol', distAux(value['features'])(col('features')))
    topK = dists.orderBy(col('distCol').desc()).limit(K_VALUE)
    predictedList = topK.select('target').collect()

    # vote for the most common category in k categories
    votes = {}
    if(len(predictedList) > 0):
      for target in predictedList[0]:
        if(target not in votes):
          votes[target] = 1
        else:
          votes[target] += 1
      # max builtin was overwritten by a pyspark module, too lazy to find which
      selectedTarget = builtins.max(votes.items(), key = itemgetter(1))[0]
      predictionsAndLabels.append((selectedTarget, value['target']))
  metricsDf = spark.createDataFrame(predictionsAndLabels,['pred','label'])
  metricsDf = metricsDf.withColumn('prediction', metricsDf.pred.cast(DoubleType()))
  metrics = MulticlassClassificationEvaluator(predictionCol='prediction', labelCol='label')
  print('\npreview do dataframe de resultados: ')
  metricsDf.select('label', 'prediction').show()
  print('precision: ', metrics.evaluate(metricsDf, {metrics.metricName :'weightedPrecision'}))
  print('recall: ', metrics.evaluate(metricsDf, {metrics.metricName :'weightedRecall'}))
  print('F1: ', metrics.evaluate(metricsDf,{metrics.metricName :'f1'}))

  print('\nCategorias estão em forma numérica para tratamento interno')

A_NN(test_values,train)