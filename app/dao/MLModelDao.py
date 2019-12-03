# encoding=utf8
from app import db
from app.models.MSEntity import MLModel
import traceback

"""
提供 ml_model（模型算子训练结果保存表） 的增删改查
"""


def create_ml_model(ml_model):
    """
    创建 新的 ml_model记录（）保存模型
    :param ml_model: 类型 [MLModel]
    :return:
    """
    try:
        session = db.session
        session.add(ml_model)
        session.commit()
        print('成功创建一个算子')
        return True

    except Exception:
        print(traceback.print_exc())
        return False


def get_ml_model(ml_model_id):
    """
    查询记录
    :param ml_model_id:
    :return:
    """

    filters = {MLModel.id == ml_model_id}
    ml_model = MLModel.query.filter(*filters).first()
    return ml_model
