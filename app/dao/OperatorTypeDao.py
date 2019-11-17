# encoding=utf8
from app.models.Mysql import OperatorType

"""
operator_type（算子种类）表 增删改查
"""


def get_all_operator_type():
    """
    查询所有的 算子种类
    :return:
    """
    return OperatorType.query.all()
