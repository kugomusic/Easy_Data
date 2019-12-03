# -*- coding: UTF-8 -*-
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import GBTClassifier, GBTClassificationModel
from pyspark.ml.feature import StringIndexer
from pyspark.sql.types import Row
from app.Utils import *

# GBDT(Gradient Boosting Decision Tree) 又叫 MART（Multiple Additive Regression Tree)，是一种迭代的决策树算法，
# 该算法由多棵决策树组成，所有树的结论累加起来做最终答案。


# GBTC API
# GBTClassifier(featuresCol='features', labelCol='label', predictionCol='prediction',
# maxDepth=5, maxBins=32, minInstancesPerNode=1, minInfoGain=0.0, maxMemoryInMB=256,
# cacheNodeIds=False, checkpointInterval=10, lossType='logistic', maxIter=20, stepSize=0.1,
# seed=None, subsamplingRate=1.0, featureSubsetStrategy='all')

trainDataRatio = 0.75  # 训练数据比例
maxIter = 20  # 迭代次数
stepSize = 0.1  # 学习速率(0-1)
maxDepth = 5  # 数的最大深度[1,100]
minInstancesPerNode = 1  # 叶子节点最少样本数[1,1000]
seed = 1  # 随机数产生器种子[0,10]
maxBins = 32  # 一个特征分裂的最大数量[1,1000]


def model_url(projectName):
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return "error: 项目名或项目路径有误"  # 错误类型：项目名或项目路径有误
    return urls['projectAddress'] + '/model/secondClassification'  # 项目地址


def gbdt(data, label_index, feature_indexs, project_url):

    # 2.构造训练数据集
    data_set = data.rdd.map(list)
    (train_data, test_data) = data_set.randomSplit([trainDataRatio, 1 - trainDataRatio])
    data.show()

    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return Row(label=label_index, features=Vectors.dense(features_data))

    training_set = train_data.map(list).map(lambda x: func(x)).toDF()
    training_set.show()
    train_num = training_set.count()
    print("训练样本数:{}".format(train_num))

    # 3.使用GBDT进行训练
    string_indexer = StringIndexer(inputCol="label", outputCol="indexed")
    si_model = string_indexer.fit(training_set)
    tf = si_model.transform(training_set)

    gbdt = GBTClassifier(labelCol="indexed",
                         maxIter=maxIter, stepSize=stepSize, maxDepth=maxDepth, minInstancesPerNode=minInstancesPerNode,
                         seed=seed)
    gbdt_model = gbdt.fit(tf)
    print(gbdt_model.featureImportances)
    # 保存模型
    model_path = project_url + '/gbdt'
    gbdt_model.write().overwrite().save(model_path)

    # 加载模型
    gbdt_model2 = GBTClassificationModel.load(model_path)

    # 预测
    gbdt_model2.transform(training_set).select("prediction", "label", "features").show(5)


userId = 1
functionName = 'gdbt'
projectName = '订单分析'
label = 0  # 标签列
features = [12, 13, 14, 15]  # 特征列
model_path = model_url(projectName)  # 项目路径

# spark会话
ss = getSparkSession(userId, functionName)
# 解析项目路径，读取csv
df = getProjectCurrentData(ss, projectName)
# if df == "error: 项目名或项目路径有误":
#     state = False
#     reason = df
#     return returnDataModel(df, state, reason)
gbdt(df, label, features, model_path)  # 二分类

# 错误 'PipelinedRDD' object has no attribute '_jdf'
# 报这个错，是因为导入的机器学习包错误所致。
# pyspark.ml 是用来处理DataFrame。
# pyspark.mllib是用来处理RDD。
