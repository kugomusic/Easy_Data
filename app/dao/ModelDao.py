# encoding=utf8
from app.models.MSEntity import Model
from app import db
import traceback
import json
import app.Utils as utils

"""
提供 model（项目） 表的增删改查
"""


def get_model_by_project_id(project_id):
    """
    通过项目ID 获取 项目对应的model
    :return:
    """

    try:
        query = db.session.query(Model).filter(Model.project_id == project_id).first()
        db.session.commit()
        return query

    except Exception:
        print(traceback.print_exc())
        return False


def update_with_project_id(project_id, start_nodes, relationship, config_order):
    """
    通过项目ID 更新 项目对应的model
    :return:
    """

    try:
        start_nodes = utils.list_str_to_list(start_nodes)
        relationship = utils.list_str_to_list(relationship)
        relationship_item_str = []
        for item in relationship:
            relationship_item_str.append(str(item))
        config = json.dumps({'config_order': config_order, 'relationship': '*,'.join(relationship_item_str)},
                            ensure_ascii=False)
        query = db.session.query(Model)
        query.filter(Model.project_id == project_id).update(
            {Model.start_nodes: ','.join(start_nodes),
             Model.config: config})
        db.session.commit()
        print('更新完成')
        return True

    except Exception:
        print(traceback.print_exc())
        return False
