from sklearn.datasets import fetch_20newsgroups
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.ml.linalg import Vectors
from pyspark.sql.types import *
from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer, Word2Vec, MinHashLSH

from pprint import pprint
# os indices estão alinhados: o valor data[1] e o filename[1] com o target[1]

spark = SparkSession.builder.appName('bgd').getOrCreate()

proc_data = fetch_20newsgroups(subset='train', remove=('headers', 'footers', 'quotes'))

test_data = fetch_20newsgroups(subset='test', remove=('headers', 'footers', 'quotes'))

# elemento 222 com problema
maxI = 250
minI = 225

raw = spark.createDataFrame(zip(proc_data.data[minI:maxI], proc_data.target.tolist()[minI:maxI]), schema=['data', 'target'])
