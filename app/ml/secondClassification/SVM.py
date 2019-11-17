# -*- coding: UTF-8 -*-
from pyspark.sql.types import *
from pyspark.mllib.classification import SVMModel
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from app.Utils import *
import numpy as np


def model_url(projectName):
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return "error: 项目名或项目路径有误"  # 错误类型：项目名或项目路径有误
    return urls['projectAddress'] + '/model/secondClassification'  # 项目地址


def svm(ss, data, label_index, feature_indexs, model_url):
    # 1. 准备数据
    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return LabeledPoint(label=np.random.randint(0, 2), features=features_data)

    training_data = data.rdd.map(lambda x: func(x))

    # 2. 训练
    svm_model = SVMWithSGD.train(training_data, iterations=20, step=1.0, regParam=0.01,
                                 miniBatchFraction=1.0, initialWeights=None, regType="l2",
                                 intercept=False, validateData=True, convergenceTol=0.001)

    # 3.预测
    predict_data = training_data.map(lambda x: x.features)
    prediction = svm_model.predict(predict_data)
    print(prediction.take(10))
    # print("真实值:{},预测值{}".format(prediction, training_data.first().label))

    # 4.保存模型
    svm_model_path = model_url + '/svm'
    deltree(svm_model_path)  # 删除已经存在的模型
    svm_model.save(ss.sparkContext, svm_model_path)

    # 5.加载模型
    same_model = SVMModel.load(ss.sparkContext, svm_model_path)


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
# svm二分类
svm(ss, df, label, features, model_path)

'''
错误 'PipelinedRDD' object has no attribute 'show'
报这个错，是因为 df.show() is only for spark DataFrame 所致。
'''
