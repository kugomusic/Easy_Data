# -*- coding: UTF-8 -*-
from flask import request, jsonify, Response
from app import app
from app.Utils import *
from app.dao.ModelDao import *
import app.service.ModelService as ModelService


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


@app.route("/model/updateFlow", methods=['POST'])
def update_flow():
    """
    新建 处理流程
    :return:
    """
    user_id = request.form.get('userId')
    project_id = request.form.get('projectId')
    config = request.form.get('config')
    start_nodes = request.form.get('startNode')
    relationship = request.form.get('relationship')
    config_order = request.form.get('configOrder')

    print('---------', user_id, project_id, config, start_nodes, relationship, config_order)
    # 更新 model（流程图）
    result = ModelService.update_model(project_id, start_nodes, config, relationship, config_order)

    if result is not False:
        return "保存成功"
    else:
        return "保存失败，请重试！"


@app.route("/model/getFlow", methods=['POST'])
def get_flow():
    """
    查看model（执行流程）
    :return:
    """
    project_id = request.form.get('projectId')
    user_id = request.form.get('userId')

    print(project_id, user_id)
    flow = ModelService.get_model_by_project_id(project_id)
    if flow is False:
        return "获取执行流程图失败，请联系工作人员"

    return flow


@app.route("/model/getRunStatus", methods=['POST'])
def get_run_status():
    """
    查看model（执行流程）中每个节点的运行状态
    :return:
    """
    project_id = request.form.get('projectId')
    user_id = request.form.get('userId')
    model_execute_id = request.form.get('modelExecuteId')

    print(project_id, user_id, model_execute_id)
    # 查看状态
    flow = ModelService.get_run_status_by_project_id(project_id, model_execute_id)

    if flow is False:
        return "获取执行流程图失败，请联系工作人员"

    return flow


@app.route("/model/executeAll", methods=['POST'])
def model_execute_all():
    """
    从model（执行流程）中的某个节点开始执行
    :return:
    """
    import _thread

    project_id = request.form.get('projectId')
    user_id = request.form.get('userId')
    print('-----/model/executeAll-----', user_id, project_id)

    try:
        param = ModelService.run_execute_status_from_start(user_id, project_id)
        _thread.start_new_thread(ModelService.model_execute, (user_id, project_id, param))
        return {'model_execute_id': param['model_execute_id']}
    except:
        traceback.print_exc()
        print("Error: 无法启动线程")
        return '启动失败'


@app.route("/model/executeFromOne", methods=['POST'])
def model_execute_from_one():
    """
    从model（执行流程）中的某个节点开始执行
    :return:
    """
    import _thread

    project_id = request.form.get('projectId')
    user_id = request.form.get('userId')
    operator_id = request.form.get('operatorId')
    print('-----/model/executeFromOne-----', user_id, project_id, operator_id)

    try:
        param = ModelService.run_execute_status_from_one(user_id, operator_id)
        _thread.start_new_thread(ModelService.model_execute, (user_id, project_id, param))
        return {'model_execute_id': param['model_execute_id']}
    except:
        print("Error: 无法启动线程")
        return '启动失败'

    return '启动成功'
