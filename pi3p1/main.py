from sklearn.datasets import fetch_20newsgroups
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, Word2Vec, MinHashLSH

from pprint import pprint
# os indices estão alinhados: o valor data[1] e o filename[1] com o target[1]

spark = SparkSession.builder.appName('bgd').master('local').getOrCreate()

proc_data = fetch_20newsgroups(subset='all', remove=('headers', 'footers', 'quotes'))

test_data = fetch_20newsgroups(subset='test', remove=('headers', 'footers', 'quotes'))

# min 246 max 250
maxI = 249
minI = 246
def vectorizeDF(raw):
  raw = spark.createDataFrame(zip(proc_data.data[minI:maxI], proc_data.target.tolist()[minI:maxI]), schema=['data', 'target'])
  removeNewLine = udf(lambda data: len(data.replace('\n', '')) > 0, BooleanType())
  raw = raw.filter(removeNewLine(col('data')))
  tokenizer = Tokenizer(inputCol='data', outputCol='tokens')
  tok_data = tokenizer.transform(raw)

  swremover = StopWordsRemover(inputCol='tokens', outputCol='words')
  rm_data = swremover.transform(tok_data)

  cv = CountVectorizer(inputCol='words', outputCol='features', vocabSize=2)
  cvmodel = cv.fit(rm_data)
  feat_data = cvmodel.transform(rm_data)
  checkZero = udf(lambda V: V.numNonzeros() > 0, BooleanType())

  feat_data = feat_data.filter(checkZero(col('features')))
  return feat_data

checkZero = udf(lambda V: V.numNonzeros(), IntegerType())

train = vectorizeDF(proc_data)
teste = vectorizeDF(test_data)

print(train.select('features').show())

# feat_data.select('rawFeatures', 'target').show()
mh = MinHashLSH(inputCol='features', outputCol='hashes', seed = 12345)
model = mh.fit(train)
# model.transform(train)



testKey = teste.take(1)[0]['features']
# print('teste: ', testKey)
# # # train.withColumn('new', checkZero(col('features'))).select('new', 'features').where(col('new') == 0).show()
model.approxNearestNeighbors(train, testKey, 2).show()