# -*- coding: UTF-8 -*-
from pyspark.ml.classification import LogisticRegression, LogisticRegressionModel, LogisticRegressionSummary
from pyspark.sql import Row
from pyspark.ml.linalg import Vectors
from app.Utils import *

'''
class pyspark.ml.classification.LogisticRegression(featuresCol='features', labelCol='label', predictionCol='prediction', 
maxIter=100, regParam=0.0, elasticNetParam=0.0, tol=1e-06, fitIntercept=True, threshold=0.5, thresholds=None, 
probabilityCol='probability', rawPredictionCol='rawPrediction', standardization=True, weightCol=None, 
aggregationDepth=2, family='auto', lowerBoundsOnCoefficients=None, upperBoundsOnCoefficients=None, 
lowerBoundsOnIntercepts=None, upperBoundsOnIntercepts=None)

featuresCol # 特征列
labelCol # 标签列
predictionCol # 预测输出列
maxIter # 最大迭代轮数
regParam # 正则化参数
elasticNetParam # ElasticNet混合参数，范围为[0，1]。对于alpha = 0，惩罚是L2惩罚。对于alpha = 1，这是L1惩罚（默认值：0.0）
tol # 迭代算法的收敛容限（> = 0）（默认值：1e-06）
fitIntercept # 是否训练截距项（默认值：True）
threshold # 二进制分类预测中的阈值，范围为[0，1]（默认值：0.5）。
thresholds # 多类别分类中的阈值，用于调整预测每个类别的概率。数组的长度必须等于类的数量，其值必须大于0，但最多一个值可以为0，这是预测值。p/t最大的类是可预测的，其中p是该类的原始概率，t是该类的概率阈值（未定义）
rawPredictionCol：原始预测（也称为置信度）列名称（默认值：rawPrediction）
standardization: whether to standardize the training features before fitting the model (default: True)
weightCol: weight column name. If this is not set or empty, we treat all instance weights as 1.0 (current: weight)
aggregationDepth # suggested depth for treeAggregate (>= 2) (default: 2)
family: The name of family which is a description of the label distribution to be used in the model. Supported options: auto, binomial, multinomial. (default: auto)
lowerBoundsOnCoefficients：如果在边界约束优化下拟合，则系数的下界。 （未定义）
lowerBoundsOnIntercepts：如果在边界约束优化下拟合，则截距的下限。 （未定义）
upperBoundsOnIntercepts：如果在边界约束优化下拟合，则截距的上限。 （未定义）

'''


def project_url(projectName):
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return "error: 项目名或项目路径有误"  # 错误类型：项目名或项目路径有误
    return urls['projectAddress']  # 项目地址


def lr(ss, data, label_index, feature_indexs, project_url):
    # 1.构造训练数据集
    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return Row(label=label_index, features=Vectors.dense(features_data))

    training_set = data.rdd.map(list).map(lambda x: func(x)).toDF()

    # 2.训练模型
    lr_param = LogisticRegression(regParam=0.01)
    lr_model = lr_param.fit(training_set)
    print(lr_model.coefficients)  # 系数
    print(lr_model.intercept)  # 截距
    print(lr_model.explainParams())  # 参数以及其注解

    # 3.保存模型
    model_path = project_url + '/model/secondClassification/lr'
    lr_model.write().overwrite().save(model_path)

    # 4.读取模型
    lr2 = lr_model.load(model_path)

    # 5.预测
    result = lr2.transform(training_set).head()
    print(result.prediction)

    sum = lr_model.summary
    print('------roc--', sum.areaUnderROC)

    # 6.评估
    # summary = lr_model.evaluate(training_set)


userId = 1
functionName = 'gdbt'
projectName = '订单分析'
label = 0  # 标签列
features = [12, 13, 14, 15]  # 特征列
project_path = project_url(projectName)  # 项目路径
# spark会话
ss = getSparkSession(userId, functionName)
# 解析项目路径，读取csv
df = getProjectCurrentData(ss, projectName)
# 罗辑回归二分类
lr(ss, df, label, features, project_path)
