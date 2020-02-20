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

maxI = 70
minI = 50

raw_groups = list(zip(proc_data.data[minI:maxI], proc_data.target.tolist()[minI:maxI]))
raw_groups[:] = [x for x in raw_groups if (len(x[0].replace('\n', '')) > 0)]

# min 246 max 250

# ver se tem pelo menos um hash igual. se tiver, usar a keyDist
# vect2 por exemplo e o vetor de teste
def keyDist(vect1, vect2):
  set1 = set(vect1)
  set2 = set(vect2)
  intersect = float(len(set1.intersection(set2)))
  union = float(len(set1.union(set2)) - intersect)
  return 1 - intersect/union


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
# teste = vectorizeDF(test_data)


# feat_data.select('rawFeatures', 'target').show()
mh = MinHashLSH(inputCol='features', outputCol='hashes', seed = 12345)
model = mh.fit(train)
train = model.transform(train)

# dataA = [(0, Vectors.sparse(10,[0,2,3,5,6,8,9],[7.0,4.0,3.0,2.0,1.0,2.0,2.0]),),
#          (2, Vectors.sparse(10,[0,1,6],[5.0,1.0,1.0]),),
#          (3, Vectors.sparse(10,[0,1,6],[5.0,1.0,1.0]),),
#          (4, Vectors.sparse(10,[0,1,4,7],[29.0,4.0,3.0,2.0]),)]
# dfA = spark.createDataFrame(dataA, ["id", "features"])

train.select('target', 'hashes').show()
# testKey = teste.take(1)[0]['features']

# ideia - > inserir as keys de treino no vetor de train. Mas na hora de calcular a keyDist separar (so para ter um hash)
# key = Vectors.sparse(10,[0],[9.0])

teste = train.select('features', 'hashes', 'target').where('id < 10')
teste.show()

train = train.select('features', 'hashes', 'target').where('id > 10')
train.show()

teste_1 = teste.take(1)

print(teste_1)

# fluxo: 
#   1 - testar se tem pelo menos um hash igual em hashes
#   2 - Calcular a distância com keyDist em cima de features
#   3 - A distância diz quao proximo é. O k do KNN diz quantos mais próximos vão determinar a classe