from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, functions

def keyDist(vect1, vect2):
  set1 = set(vect1)
  set2 = set(vect2)
  print(set1)
  print(set2)
  intersectSize = len(set1.intersection(set2))
  union = float(len(set1) + len(set2) - intersectSize)
  return 1 - intersectSize/union

spark = SparkSession.builder.appName('bgd').getOrCreate()


dataA = [Vectors.sparse(10,[0,2,3,5,6,8,9],[7.0,4.0,3.0,2.0,1.0,2.0,2.0]),
         Vectors.sparse(10,[0,1,6],[5.0,1.0,1.0]),
         Vectors.sparse(10,[0,1,4],[5.0,1.0,4.0]),
         Vectors.sparse(10,[0,1,4,7],[29.0,4.0,3.0,2.0])]

d1 = dataA[:2]
d2 = dataA[2:]

print(len(d1[1]))
print(d2[0])

print(keyDist(d1[1], d2[0]))

# dfA = spark.createDataFrame(dataA, ["id", "features"])

# key = Vectors.sparse(10,[0],[9.0])

# mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
# model = mh.fit(dfA)

# print(dfA.dtypes)

# # Feature Transformation
# print("The hashed dataset where hashed values are stored in the column 'hashes':")
# dfa = model.transform(dfA)

# # Compute the locality sensitive hashes for the input rows, then perform approximate
# # similarity join.
# # We could avoid computing hashes by passing in the already-transformed dataset, e.g.
# # `model.approxSimilarityJoin(transformedA, transformedB, 0.6)`
# # print("Approximately joining dfA and dfB on distance smaller than 0.6:")
# # model.approxSimilarityJoin(dfA, dfB, 0.6, distCol="JaccardDistance")\
# #     .select(col("datasetA.id").alias("idA"),
# #             col("datasetB.id").alias("idB"),
# #             col("JaccardDistance")).show()

# # Compute the locality sensitive hashes for the input rows, then perform approximate nearest
# # neighbor search.
# # We could avoid computing hashes by passing in the already-transformed dataset, e.g.
# # `model.approxNearestNeighbors(transformedA, key, 2)`
# # It may return less than 2 rows when not enough approximate near-neighbor candidates are
# # found.
# print("Approximately searching dfA for 2 nearest neighbors of the key:")
# model.approxNearestNeighbors(dfA, key, 4).show()