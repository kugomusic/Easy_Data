# -*- coding: UTF-8 -*-

"""
模型评估
"""
import app.dao.OperatorDao as OperatorDao
from pyspark.mllib.classification import SVMModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.evaluation import BinaryClassificationMetrics
from app.Utils import *


def second_evaluation(spark_session, operator_id, condition):
    """
    二分类评估
    :param spark_session:
    :param operator_id:
    :param condition:
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 评估函数
        result_df = second_evaluation_core(spark_session, condition, operator_id)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_df.show()
            result_file_url = save_data(result_df)
            run_info = '评估算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def second_evaluation_core(spark_session, condition, operator_id):
    """
    二分类评估核心函数
    :param spark_session:
    :param condition:
    :param operator_id:
    :return:
    """
    # 读模型
    # 当前节点（评估节点）一个父节点
    operator = OperatorDao.get_operator_by_id(operator_id)
    # 父节点(预测节点) 两个父节点
    father_id = operator.father_operator_ids
    father_operator = OperatorDao.get_operator_by_id(father_id)
    # 祖节点（模型节点和读预测数据节点）
    grand_father_ids = father_operator.father_operator_ids.split(',')
    print("**********祖节点（模型节点和读预测数据源节点）:", grand_father_ids)

    # 读数据
    def get_predict_data(operator_config_):
        for grand_father_file_ in operator_config_:
            grand_father_id_ = list(grand_father_file_.keys())[0]
            grand_father_ = OperatorDao.get_operator_by_id(grand_father_id_)
            if grand_father_.operator_type_id == 5001 or grand_father_.operator_type_id < 3000:
                print("***************评估函数，预测数据：", grand_father_.operator_type_id)
                pre_data_file_url = grand_father_.operator_output_url.split('*,')[
                    grand_father_file_[grand_father_id_]]
                print("***************评估函数，预测数据url：", pre_data_file_url)
                return read_data(spark_session, pre_data_file_url)

    print("**********预测节点:", father_operator.operator_config)
    df = get_predict_data(json.loads(father_operator.operator_config)['fileUrl'])

    # 评估
    for grand_father_id in grand_father_ids:
        grand_father = OperatorDao.get_operator_by_id(grand_father_id)
        grand_father_operator_type = grand_father.operator_type_id
        # 模型加载节点
        if grand_father_operator_type == 8000:
            grand_father_operator_type = json.loads(grand_father.operator_config)['parameter']['modelTypeId']
        if grand_father_operator_type == 6001:  # svm二分类节点
            print("***************评估函数，训练模型", grand_father.operator_type_id)
            evaluation_df = svm_second_evaluation(spark_session, grand_father.operator_output_url, df,
                                                  json.loads(father_operator.operator_config)['parameter'], condition)
            return evaluation_df


def svm_second_evaluation(spark_session, svm_model_path, df, predict_condition, condition):
    """
    svm二分类评估
    :param spark_session:
    :param svm_model_path: 模型地址
    :param df: 预测数据
    :param predict_condition: 预测算子（父算子）配置
    :param condition: 该算子配置 {"label":"标签"}
    :return:
    """

    feature_indexs = predict_condition['features']
    label = condition['label']

    # 1. 准备数据
    def func(x):
        features_data = []
        for feature in feature_indexs:
            features_data.append(x[feature])
        return LabeledPoint(label=x[label], features=features_data)

    predict_data = df.rdd.map(lambda x: func(x))

    # 加载模型
    svm_model = SVMModel.load(spark_session.sparkContext, svm_model_path)

    # 计算评估指标
    svmTotalCorrect = predict_data.map(lambda r: 1 if (svm_model.predict(r.features) == r.label) else 0).reduce(
        lambda x, y: x + y)
    svmAccuracy = svmTotalCorrect / float(predict_data.count())

    # 清除默认阈值，这样会输出原始的预测评分，即带有确信度的结果
    svm_model.clearThreshold()
    svmPredictionAndLabels = predict_data.map(lambda lp: (float(svm_model.predict(lp.features)), lp.label))
    svmMetrics = BinaryClassificationMetrics(svmPredictionAndLabels)
    print("Area under PR = %s" % svmMetrics.areaUnderPR)
    print("Area under ROC = %s" % svmMetrics.areaUnderROC)

    # 返回数据
    result = [("正确个数", float(svmTotalCorrect)),
              ("精准度", float(svmAccuracy)),
              ("Area under PR", float(svmMetrics.areaUnderPR)),
              ("Area under ROC", float(svmMetrics.areaUnderROC))]
    return spark_session.createDataFrame(result, schema=['指标', '值'])
