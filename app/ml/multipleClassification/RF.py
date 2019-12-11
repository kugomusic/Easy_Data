# -*- coding: UTF-8 -*-
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, LogisticRegressionSummary, \
    LogisticRegressionTrainingSummary, RandomForestClassifier
from pyspark.sql import Row, DataFrame
from pyspark.ml.linalg import Vectors
from app.Utils import *

'''
    It supports both binary and multiclass labels, as well as both continuous and categoricaleatures.

'''


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass

    return False


def project_url(projectName):
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return "error: 项目名或项目路径有误"  # 错误类型：项目名或项目路径有误
    return urls['projectAddress']  # 项目地址


def rf(ss, data, label_index, feature_indexs, project_url):
    # 1.构造训练数据集
    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return Row(label=label_index, features=Vectors.dense(features_data))

    training_set = data.rdd.map(list).map(lambda x: func(x)).toDF()

    # 2.训练模型
    rf_param = RandomForestClassifier(numTrees=50)
    rf_model = rf_param.fit(training_set)

    # 3.保存模型
    model_path = project_url + '/model/multipleClassification/rf'
    rf_model.write().overwrite().save(model_path)

    # 4.读取模型
    rf2 = rf_model.load(model_path)

    # 5.预测
    rf_pred = rf2.transform(training_set)
    rf_pred.select("prediction", "features").show()

    from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    # 6.评估
    rf_accuracy = MulticlassClassificationEvaluator(metricName='accuracy').evaluate(rf_pred)
    print("RF's accuracy is %f" % rf_accuracy)
    rf_precision = MulticlassClassificationEvaluator(metricName='weightedPrecision').evaluate(rf_pred)
    print("RF's precision is %f" % rf_precision)


userId = 1
functionName = 'lr'
projectName = '订单分析'
label = 0  # 标签列
features = [2, 4, 10, 11, 12]  # 特征列
project_path = project_url(projectName)  # 项目路径
# spark会话
ss = getSparkSession(userId, functionName)
# 解析项目路径，读取csv
fileUrl = '/home/zk/data/adult.csv'
df = ss.read.csv(fileUrl)
df.filter
print(df.dtypes)

df.show()
# df = getProjectCurrentData(ss, projectName)
# 罗辑回归二分类
rf(ss, df, label, features, project_path)
