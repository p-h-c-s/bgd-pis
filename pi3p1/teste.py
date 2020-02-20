from pyspark.ml.feature import MinHashLSH
from pyspark.ml.linalg import Vectors
from pyspark.sql.functions import col
from pyspark.sql import SparkSession, functions


spark = SparkSession.builder.appName('bgd').getOrCreate()


dataA = [(0, Vectors.sparse(2,[0],[9.0]),),
         (2, Vectors.sparse(2,[0,1],[84.0,14.0]),)]
dfA = spark.createDataFrame(dataA, ["id", "features"])

key = Vectors.sparse(2, [0, 1], [1.0, 1.0])

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
model = mh.fit(dfA)

print(dfA.dtypes)

# Feature Transformation
print("The hashed dataset where hashed values are stored in the column 'hashes':")
dfa = model.transform(dfA)

# Compute the locality sensitive hashes for the input rows, then perform approximate
# similarity join.
# We could avoid computing hashes by passing in the already-transformed dataset, e.g.
# `model.approxSimilarityJoin(transformedA, transformedB, 0.6)`
# print("Approximately joining dfA and dfB on distance smaller than 0.6:")
# model.approxSimilarityJoin(dfA, dfB, 0.6, distCol="JaccardDistance")\
#     .select(col("datasetA.id").alias("idA"),
#             col("datasetB.id").alias("idB"),
#             col("JaccardDistance")).show()

# Compute the locality sensitive hashes for the input rows, then perform approximate nearest
# neighbor search.
# We could avoid computing hashes by passing in the already-transformed dataset, e.g.
# `model.approxNearestNeighbors(transformedA, key, 2)`
# It may return less than 2 rows when not enough approximate near-neighbor candidates are
# found.
print("Approximately searching dfA for 2 nearest neighbors of the key:")
model.approxNearestNeighbors(dfA, key, 2).show()