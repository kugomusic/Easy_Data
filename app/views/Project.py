# encoding=utf8
import sys
from importlib import reload

reload(sys)

from flask import request, jsonify, Response
from flask.json import jsonify
from app import app
from app import db
from app.models.MSEntity import DataSource, Project, Model
import os
from app.Utils import mkdir, getProjectByNameAndUserId
from app.ConstFile import const


class MyResponse(Response):
    """解决 list, dict 不能返回的问题"""

    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


@app.route('/project/testList', methods=['GET', 'POST'])
def test_list():
    """
    获取项目列表
    :return:
    """
    result = []

    return jsonify(result)


@app.route('/project/getAll', methods=['GET', 'POST'])
def get_all():
    """
    获取项目列表
    :return:
    """
    data_sources = Project.query.all()
    result = []
    for i in data_sources:
        result.append({"id": i.id, "name": i.project_name})
    return jsonify(result)


@app.route('/project/create', methods=['GET', 'POST'])
def create():
    """
    创建项目。
    创建项目的时候 创建一个model，现在项目和model是 1：1对应的关系

    :return:
    """
    if request.method == 'GET':
        projectName = request.form.get('projectName')
        userId = request.form.get('userId')
    else:
        projectName = request.form.get('projectName')
        userId = request.form.get('userId')
    # 弃用，因此默认 1
    data_source_id = 1
    print('projectName: {}, dataSourceId: {}, userId: {}'.format(projectName, data_source_id, userId))

    root_url = const.ROOTURL

    # 数据库中添加Project记录
    project = Project(project_name=projectName, project_address=root_url + projectName, user_id=userId,
                      dataSource_id=data_source_id)
    db.session.add(project)

    # 数据库中添加Model记录
    # 格式化成2016-03-20 11:45:39形式
    import time
    project = getProjectByNameAndUserId(projectName, userId)
    model = Model(model_name=projectName, project_id=project.id, start_nodes="",
                  create_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    db.session.add(model)
    db.session.commit()

    # 创建项目目录
    try:
        if not (os.path.exists(root_url + projectName)):
            filters = {
                DataSource.id == data_source_id
            }
            data_sources = DataSource.query.filter(*filters).first()
            db.session.commit()
            mkdir(root_url + projectName)
            print(data_sources.file_url)
            return get_all()
        else:
            return "Double name"
    except:
        return "error"
