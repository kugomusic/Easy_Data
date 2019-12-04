# -*- coding: UTF-8 -*-
"""
多分类
"""

import app.dao.OperatorDao as OperatorDao
from app.Utils import *
from pyspark.ml.linalg import Vectors
from pyspark.ml.classification import LogisticRegression
from pyspark.sql.types import Row


def model_url():
    """
    二分类模型保存地址
    :return:
    """
    return const.MIDDATA + 'model/multipleClassification'


def lr(spark_session, operator_id, file_url, condition):
    """
    逻辑回归多分类
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition:
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # svm_core函数
        result_model_url = lr_core(df, condition)
        # 修改计算状态
        run_info = '逻辑回归多分类算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', result_model_url, run_info)
        return [result_model_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def lr_core(df, condition):
    """
    lr多分类核心函数
    :param spark_session:
    :param df:
    :param condition:{"label": "标签", "features": ["数量", "折扣", "利润", "装运成本"], "iterations": 20,"regParam":0.0,"elasticNetParam":0.0,"tol":0.000006,"fitIntercept":True}
    :return:
    """
    # 参数
    label_index = condition['label']  # 标签列(列名或列号)
    feature_indexs = condition['features']  # 特征列(列名或列号)
    iterations = condition['iterations']  # 最大迭代次数(默认100)
    regParam = condition['regParam']  # 正则化参数（默认0.0）
    # ElasticNet混合参数，范围为[0，1]。对于alpha = 0，惩罚是L2惩罚。对于alpha = 1，这是L1惩罚（默认值：0.0)
    elasticNetParam = condition['elasticNetParam']
    tol = condition['tol']  # 迭代算法的收敛容限（> = 0）（默认值：1e-06即 0.000006）
    fitIntercept = condition['fitIntercept']  # 是否训练截距项（默认值："True","False"可选)

    # 参数类型转换
    if isinstance(iterations, str):
        iterations = int(iterations)
    if isinstance(regParam, str):
        regParam = float(regParam)
    if isinstance(elasticNetParam, str):
        elasticNetParam = float(elasticNetParam)
    if isinstance(tol, str):
        tol = float(tol)
    if isinstance(fitIntercept, str):
        if fitIntercept == 'False':
            fitIntercept = False
        else:
            fitIntercept = True

    # 1. 准备数据
    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return Row(label=x[label_index], features=Vectors.dense(features_data))

    training_set = df.rdd.map(lambda x: func(x)).toDF()

    # 2.训练模型
    lr_param = LogisticRegression(featuresCol="features", labelCol="label", predictionCol="prediction",
                                  maxIter=iterations, regParam=regParam, elasticNetParam=elasticNetParam, tol=tol,
                                  fitIntercept=fitIntercept, probabilityCol="probability",
                                  rawPredictionCol="rawPrediction", standardization=True, aggregationDepth=2,
                                  family="multinomial")
    lr_model = lr_param.fit(training_set)
    print(lr_model.coefficientMatrix)  # 系数
    print(lr_model.interceptVector)  # 截距
    print(lr_model.explainParams())  # 参数以及其注解

    # 3.保存模型
    lr_model_path = model_url() + '/lr/' + str(uuid.uuid1())
    deltree(lr_model_path)  # 删除已经存在的模型
    lr_model.write().overwrite().save(lr_model_path)

    return lr_model_path
