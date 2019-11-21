# encoding=utf8
from app.models.MSEntity import Project

"""
提供 project（项目） 表的增删改查
"""


def get_project_by_id(project_id):
    """
    通过项目ID获取项目
    :return:
    """

    try:
        filters = {
            Project.id == project_id,
        }
        return Project.query.filter(*filters).first()
    except Exception:
        return "error"
