# encoding=utf8
from app.models.MSEntity import Operator
from app import db
import traceback

"""
operator（算子）表 增删改查
"""


def update_operator_by_id(operator_id, status, operator_output_url="", run_info=""):
    """
    通过 operator_id 更新 operator的执行状态、结果保存路径、运行信息
    :param operator_id:
    :param status:
    :param operator_output_url:
    :param run_info:
    :return:
    """
    try:
        filters = {
            Operator.id == operator_id,
        }
        result = Operator.query.filter(*filters).first()
        result.status = status
        result.operator_output_url = operator_output_url
        result.run_info = run_info
        db.session.commit()
        return True
    except Exception as e:
        print(traceback.print_exc())
        return False


def update_operator_input_url(operator_id, operator_input_url):
    """
    通过 operator_id 更新 operator的输入路径
    :param operator_id:
    :param operator_input_url:
    :return:
    """
    try:
        filters = {
            Operator.id == operator_id,
        }
        result = Operator.query.filter(*filters).first()
        result.operator_input_url = operator_input_url
        db.session.commit()
        return True
    except Exception as e:
        print(traceback.print_exc())
        return False


def get_operator_by_id(operator_id):
    """
    通过 id 查询 operator
    :param operator_id:
    :return:
    """
    try:
        query = db.session.query(Operator).filter(Operator.id == operator_id).first()
        db.session.commit()
        return query
    except Exception:
        print(traceback.print_exc())
        return False


def get_operator_by_ids(operator_ids):
    """
    通过 id集合 查询 operator
    :param operator_ids:[]
    :return:
    """
    try:
        query = db.session.query(Operator).filter(Operator.id in operator_ids)
        db.session.commit()
        return query
    except Exception:
        print(traceback.print_exc())
        return False


def get_operator_by_model_id(model_id):
    """
    通过 model_id 查询 operator
    :param model_id:
    :return:
    """
    try:
        query = db.session.query(Operator).filter(Operator.model_id == model_id)
        db.session.commit()
        return query
    except Exception:
        print(traceback.print_exc())
        return False


def delete_operator_by_model_id(model_id):
    """
    通过 model_id 删除 operator
    :param model_id:
    :return:
    """
    try:
        db.session.query(Operator).filter(Operator.model_id == model_id).delete()
        db.session.commit()
        return True
    except Exception:
        print(traceback.print_exc())
        return False


def create_operator(operators):
    """
    创建 新的operator（算子）
    :param operators: 类型 [Operator]
    :return:
    """
    try:
        session = db.session
        session.add_all(operators)
        session.commit()
        print('成功创建一个算子')
        return True

    except Exception:
        print(traceback.print_exc())
        return False
