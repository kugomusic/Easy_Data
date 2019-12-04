# -*- coding: UTF-8 -*-
from pyspark.ml.classification import MultilayerPerceptronClassifier, MultilayerPerceptronClassificationModel
from pyspark.sql import Row, DataFrame
from pyspark.ml.linalg import Vectors
from app.Utils import *


# userId = 1
# functionName = 'gdbt'
# projectName = '订单分析'
# # spark会话
# spark = getSparkSession(userId, functionName)
# df = spark.createDataFrame([
#     (0.0, Vectors.dense([0.0, 0.0])),
#     (1.0, Vectors.dense([0.0, 1.0])),
#     (1.0, Vectors.dense([1.0, 0.0])),
#     (0.0, Vectors.dense([1.0, 1.0]))], ["label", "features"])
# mlp = MultilayerPerceptronClassifier(maxIter=100, layers=[2, 2, 2], blockSize=1, seed=123)
# model = mlp.fit(df)
# print(model.layers)
# print(model.weights.size)
# testDF = spark.createDataFrame([
#     (Vectors.dense([1.0, 0.0]),),
#     (Vectors.dense([0.0, 0.0]),)], ["features"])
# model.transform(testDF).select("features", "prediction").show()


def project_url(projectName):
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return "error: 项目名或项目路径有误"  # 错误类型：项目名或项目路径有误
    return urls['projectAddress']  # 项目地址


def mpc(ss, data, label_index, feature_indexs, project_url):
    # 1.构造训练数据集
    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return Row(label=label_index, features=Vectors.dense(features_data))

    training_set = data.rdd.map(lambda x: func(x)).toDF()

    # 2.训练模型
    # maxIter=100, tol=1e-6, seed=None, layers=None, blockSize=128, stepSize=0.03, solver="l-bfgs", initialWeights=None
    mpc_param = MultilayerPerceptronClassifier(maxIter=100, tol=1e-6, blockSize=128, stepSize=0.03, solver="l-bfgs")
    mpc_param.setSeed(1)
    mpc_param.setLayers([4, 2, 2])
    mpc_model = mpc_param.fit(training_set)

    # 3.保存模型
    model_path = project_url + '/model/multipleClassification/mpc'
    mpc_model.write().overwrite().save(model_path)

    # 4.读取模型
    mpc2 = MultilayerPerceptronClassificationModel.load(model_path)

    # 5.预测
    result = mpc2.transform(training_set).select("prediction", "features").show()


userId = 1
functionName = 'mpc'
projectName = '订单分析'
label = 0  # 标签列
features = [12, 13, 14, 15]  # 特征列
project_path = project_url(projectName)  # 项目路径
# spark会话
ss = getSparkSession(userId, functionName)
# 解析项目路径，读取csv
df = getProjectCurrentData(ss, projectName)
# 罗辑回归二分类
mpc(ss, df, label, features, project_path)
