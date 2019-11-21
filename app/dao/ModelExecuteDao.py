# encoding=utf8
from app.models.MSEntity import ModelExecute
from app import db
import traceback

"""
模型执行表 增删改查
"""


def get_model_execute_by_id(model_execute_id):
    """
    通过 id 查询 model_execute
    :param model_execute_id:
    :return:
    """
    try:
        query = db.session.query(ModelExecute).filter(ModelExecute.id == model_execute_id).first()
        db.session.commit()
        return query
    except Exception:
        print(traceback.print_exc())
        return False


def create_model_execute(model_execute):
    """
    创建 model_execute
    :param model_execute: 类型 ModelExecute
    :return:
    """
    try:
        session = db.session
        session.add(model_execute)
        session.commit()
        print('成功创建一条执行记录')
        return model_execute
    except Exception:
        print(traceback.print_exc())
        return False


def update_model_execute(model_execute_id, status, run_info, end_time):
    """
    更新 model_execute

    :param model_execute_id:
    :param status:
    :param run_info:
    :param end_time:
    :return:
    """
    try:
        filters = {
            ModelExecute.id == model_execute_id,
        }
        result = ModelExecute.query.filter(*filters).first()
        result.status = status
        result.run_info = run_info
        result.end_time = end_time
        db.session.commit()
        return True
    except Exception as e:
        print(traceback.print_exc())
        return False
