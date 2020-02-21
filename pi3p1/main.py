from sklearn.datasets import fetch_20newsgroups
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, Word2Vec, MinHashLSH
import math

from pprint import pprint
# os indices estão alinhados: o valor data[1] e o filename[1] com o target[1]

spark = SparkSession.builder.appName('bgd').master('local').getOrCreate()

proc_data = fetch_20newsgroups(subset='all', remove=('headers', 'footers', 'quotes'))

test_data = fetch_20newsgroups(subset='test', remove=('headers', 'footers', 'quotes'))

maxI = 100
minI = 90
raw_groups = list(zip(proc_data.data, proc_data.target.tolist()))
raw_groups[:] = [x for x in raw_groups if (len(x[0].replace('\n', '')) > 0)]

# min 246 max 250
def vectorizeDF(raw):
  raw = spark.createDataFrame(raw_groups, schema=['data', 'target'])
  raw = raw.withColumn('id', monotonically_increasing_id())

  tokenizer = Tokenizer(inputCol='data', outputCol='tokens')
  tok_data = tokenizer.transform(raw)

  swremover = StopWordsRemover(inputCol='tokens', outputCol='words')
  rm_data = swremover.transform(tok_data)

  cv = CountVectorizer(inputCol='words', outputCol='features', vocabSize=10)
  cvmodel = cv.fit(rm_data)
  feat_data = cvmodel.transform(rm_data)
  checkZero = udf(lambda V: V.numNonzeros() > 0, BooleanType())

  feat_data = feat_data.filter(checkZero(col('features')))
  return feat_data

train = vectorizeDF(proc_data)

mh = MinHashLSH(inputCol='features', outputCol='hashes', numHashTables=1, seed = 12345)
model = mh.fit(train)
train = model.transform(train)

extractExploded = udf(lambda l: float(l[0]), FloatType())
# extract hash values from matrix
train = train.select('id', 'features', 'target', explode('hashes').alias('hashes'))
# cast hash values to float then extract from denseVector
train = train.withColumn('extracted', extractExploded(col('hashes')))

# ideia - > inserir as keys de treino no vetor de train. Mas na hora de calcular a keyDist separar (so para ter um hash)

teste = train.select('id','features', 'extracted', 'target').where('id < 5')
print('dados de teste')
teste.show()

train = train.select('id', 'features', 'extracted', 'target').where('id >= 5')
print('dados de treino')
train.show()

teste_1 = teste.take(1)
teste_1_feat = teste_1[0]['features']
teste_1 = float(teste_1[0]['extracted'])

print(teste_1_feat)
print(teste_1)


print('hashEncounters com {}'.format(teste_1))
# assumindo que o spark sabe comparar floats de maneira decente
hashEncounters = train.where('extracted == {}'.format(teste_1))


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

dists = hashEncounters.withColumn('distCol', distAux(teste_1_feat)(col('features')))

dists.show()

# fluxo: 
#   1 - testar se tem pelo menos um hash igual em hashes
#   2 - Calcular a distância com keyDist em cima de features
#   3 - A distância diz quao proximo é. O k do KNN diz quantos mais próximos vão determinar a classe