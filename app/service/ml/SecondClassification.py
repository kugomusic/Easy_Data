# -*- coding: UTF-8 -*-
"""
二分类
"""
from pyspark.mllib.classification import SVMWithSGD
from pyspark.mllib.regression import LabeledPoint
import app.dao.OperatorDao as OperatorDao
from app.Utils import *


def model_url():
    """
    二分类模型保存地址
    :return:
    """
    return const.MIDDATA + '/model/secondClassification'


def svm(spark_session, operator_id, file_url, condition):
    """
    支持向量机二分类
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
        result_model_url = svm_core(spark_session, df, condition)
        # 修改计算状态
        run_info = '支持向量机二分类算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', result_model_url, run_info)
        return [result_model_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def svm_core(spark_session, df, condition):
    """
    支持向量机二分类核心函数
    :param spark_session:
    :param df:
    :param condition:
    {"label": "", "features": [12, 13, 14, 15], "iterations": 20, "step": 1.0, "regParam": 0.01, "regType": "l2", "convergenceTol": 0.001}
    :return:
    """

    # 参数
    label_index = condition['label']  # 标签列(列名或列号)
    feature_indexs = condition['features']  # 特征列(列名或列号)
    iterations = condition['iterations']  # 迭代轮数
    step = condition['step']  # 步长
    reg_param = condition['regParam']  # 正则化系数
    reg_type = condition['regType']  # 正则化
    convergence_tol = condition['convergenceTol']  # 收敛系数

    # 1. 准备数据
    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return LabeledPoint(label=x[label_index], features=features_data)

    training_data = df.rdd.map(lambda x: func(x))

    # 2. 训练
    svm_model = SVMWithSGD.train(training_data, iterations=iterations, step=step, regParam=reg_param,
                                 miniBatchFraction=1.0, initialWeights=None, regType=reg_type,
                                 intercept=False, validateData=True, convergenceTol=convergence_tol)

    # 3.保存模型
    svm_model_path = model_url() + '/svm/' + str(uuid.uuid1())
    deltree(svm_model_path)  # 删除已经存在的模型
    svm_model.save(spark_session.sparkContext, svm_model_path)

    return svm_model_path


'''
错误 'PipelinedRDD' object has no attribute 'show'
报这个错，是因为 df.show() is only for spark DataFrame 所致。
'''
