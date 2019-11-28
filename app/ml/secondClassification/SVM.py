# -*- coding: UTF-8 -*-
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

    # # 4.保存模型
    # svm_model_path = model_url + '/svm'
    # deltree(svm_model_path)  # 删除已经存在的模型
    # svm_model.save(ss.sparkContext, svm_model_path)
    #
    # # 5.加载模型
    # same_model = SVMModel.load(ss.sparkContext, svm_model_path)

    # 6.模型评估
    evl(training_data, svm_model)


def evl(data, svmModel):
    ## 正确率和错误率
    # lrTotalCorrect = data.map(lambda r: 1 if (lrModel.predict(r.features) == r.label) else 0).reduce(lambda x, y: x + y)
    # lrAccuracy = lrTotalCorrect / float(data.count())  # 0.5136044023234485

    svmTotalCorrect = data.map(lambda r: 1 if (svmModel.predict(r.features) == r.label) else 0).reduce(
        lambda x, y: x + y)
    svmAccuracy = svmTotalCorrect / float(data.count())  # 0.5136044023234485
    #
    # nbTotalCorrect = data_for_bayes.map(lambda r: 1 if (bayesModel.predict(r.features) == r.label) else 0).reduce(
    #     lambda x, y: x + y)
    # nbAccuracy = nbTotalCorrect / float(data_for_bayes.count())  # 0.5799449709568939

    # dt_predictions = dtModel.predict(data.map(lambda x: x.features))
    # labelsAndPredictions = data.map(lambda x: x.label).zip(dt_predictions)
    # dtTotalCorrect = labelsAndPredictions.map(lambda r: 1 if (r[0] == r[1]) else 0).reduce(lambda x, y: x + y)
    # dtAccuracy = dtTotalCorrect / float(data.count())  # 0.654234179150107

    # Compute raw scores on the test set
    # lrPredictionAndLabels = data.map(lambda lp: (float(lrModel.predict(lp.features)), lp.label))
    # # Instantiate metrics object
    # lrmetrics = BinaryClassificationMetrics(lrPredictionAndLabels)
    # # Area under precision-recall curve
    # print("Area under PR = %s" % lrmetrics.areaUnderPR)
    # # Area under ROC curve
    # print("Area under ROC = %s" % lrmetrics.areaUnderROC)

    # 清除默认阈值，这样会输出原始的预测评分，即带有确信度的结果
    svmModel.clearThreshold()
    predict_data = data.map(lambda x: x.features)
    prediction = svmModel.predict(predict_data)
    print(prediction.take(10))

    svmPredictionAndLabels = data.map(lambda lp: (float(svmModel.predict(lp.features)), lp.label))
    svmMetrics = BinaryClassificationMetrics(svmPredictionAndLabels)
    print("Area under PR = %s" % svmMetrics.areaUnderPR)
    print("Area under ROC = %s" % svmMetrics.areaUnderROC)

    # bayesPredictionAndLabels = data_for_bayes.map(lambda lp: (float(bayesModel.predict(lp.features)), lp.label))
    # bayesMetrics = BinaryClassificationMetrics(bayesPredictionAndLabels)
    # print("Area under PR = %s" % bayesMetrics.areaUnderPR)
    # print("Area under ROC = %s" % bayesMetrics.areaUnderROC)


userId = 1
functionName = 'gdbt'
projectName = '订单分析'
label = 0  # 标签列
features = ["数量", "折扣", "利润", "装运成本"]  # 特征列
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
