# -*- coding: UTF-8 -*-

import app.dao.ModelDao as ModelDao
import app.dao.MLModelDao as MLModelDao
import app.dao.OperatorDao as OperatorDao
import app.dao.OperatorTypeDao as OperatorTypeDao
import app.dao.ModelExecuteDao as ModelExecuteDao
from app.models.MSEntity import Operator, ModelExecute, MLModel
import app.service.ModelExecuteService as ModelExecuteService
from app.Utils import *

"""
关于ml_model 的处理方法
"""


def save_ml_model(operator_id, user_id, name):
    """
    保存训练模型
    :param operator_id:
    :param user_id:
    :param name:
    :return:
    """
    # 查看算子
    operator = OperatorDao.get_operator_by_id(operator_id)
    if operator.operator_type_id > 7000 or operator.operator_type_id < 6001:
        return "所选择的节点并不是模型算子节点"
    if operator.status != "success":
        return "请执行该节点"
    if operator.operator_output_url is not None:
        operator_output_url = operator.operator_output_url.split('*,')
    else:
        return "没有运行结果"

    model_url = operator_output_url[0]
    operator_type_id = operator.operator_type_id
    model_id = operator.model_id

    # 查看执行流程model
    model = ModelDao.get_model_by_id(model_id)
    project_id = model.project_id

    ml_model = MLModel(user_id=user_id, project_id=project_id, model_id=model_id, status='save', name=name,
                       operator_type_id=operator_type_id, model_url=model_url)
    return MLModelDao.create_ml_model(ml_model)


def get_ml_model(ml_model_id, project_id, user_id, model_id, name, status):
    """
    按照条件查询ml_model
    :param ml_model_id:
    :param project_id:
    :param user_id:
    :param model_id:
    :param name:
    :param status:
    :return:
    """
    ml_models = MLModel.query
    if (ml_model_id is not None) and (ml_model_id is not ''):
        ml_models = ml_models.filter(MLModel.id == ml_model_id)
    if (project_id is not None) and (project_id is not ''):
        ml_models = ml_models.filter(MLModel.project_id == project_id)
    if (user_id is not None) and (user_id is not ''):
        ml_models = ml_models.filter(MLModel.user_id == user_id)
    if (model_id is not None) and (model_id is not ''):
        ml_models = ml_models.filter(MLModel.model_id == model_id)
    if (name is not None) and (name is not ''):
        ml_models = ml_models.filter(MLModel.name == name)
    if (status is not None) and (status is not ''):
        ml_models = ml_models.filter(MLModel.status == status)
    results = []
    for ml_model in ml_models:
        results.append({"MLModelId": ml_model.id, "status": ml_model.status, "name": ml_model.name,
                        "operatorTypeId": ml_model.operator_type_id})
    return results


def delete_ml_model(ml_model_id):
    """
    删除 ml_model
    :param ml_model_id:
    :return:
    """
    filters = {MLModel.id: ml_model_id}
    MLModel.query.filter(*filters).delete()
    db.session.commit()
