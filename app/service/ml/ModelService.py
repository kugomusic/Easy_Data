"""
模型加载
"""
import app.dao.OperatorDao as OperatorDao
import app.dao.MLModelDao as MLModelDao
from app.Utils import *


def model_operator(operator_id, condition):
    """
    加载模型算子
    :param operator_id:
    :param condition:{"MLModelId": 2, "modelTypeId": 6001}
    :return:
    """

    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 评估函数
        model_file_url = model_operator_core(condition)
        # 修改计算状态
        run_info = '模型算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', model_file_url, run_info)
        return [model_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def model_operator_core(condition):
    # 查询 ml_model
    ml_model_id = condition['MLModelId']
    ml_model = MLModelDao.get_ml_model(ml_model_id)
    return ml_model.model_url
