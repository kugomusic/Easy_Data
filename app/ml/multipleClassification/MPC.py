# -*- coding: UTF-8 -*-
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.sql import Row, DataFrame
from pyspark.ml.linalg import Vectors
from app.Utils import *

userId = 1
functionName = 'gdbt'
projectName = '订单分析'
# spark会话
spark = getSparkSession(userId, functionName)
df = spark.createDataFrame([
    (0.0, Vectors.dense([0.0, 0.0])),
    (1.0, Vectors.dense([0.0, 1.0])),
    (1.0, Vectors.dense([1.0, 0.0])),
    (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
mlp = MultilayerPerceptronClassifier(maxIter=100, layers=[2, 2, 2], blockSize=1, seed=123)
model = mlp.fit(df)
print(model.layers)
print(model.weights.size)
testDF = spark.createDataFrame([
    (Vectors.dense([1.0, 0.0]),),
    (Vectors.dense([0.0, 0.0]),)], ["features"])
model.transform(testDF).select("features", "prediction").show()
