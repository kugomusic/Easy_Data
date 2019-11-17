# -*- coding: UTF-8 -*-
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, \
    abort
from flask.json import jsonify
from app import app
import json
import os
import time
from app.Utils import *
from app.dao.ModelDao import *
import app.service.ModelService as ModelService
import app.dao.ProjectDao as ProjectDao

from app.views import Process
import app.Utils as apus
import pandas as pd
from pyspark.sql import SparkSession
import random
import string


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


# # 解析filter参数函数
# def parsingFilterParameters(str):
#     condition = []
#     strList = str.split(';')
#     for i in range(len(strList)):
#         ll = strList[i].split(',', 3)
#         con = {}
#         con['name'] = ll[0]
#         con['operate'] = ll[1]
#         con['value'] = ll[2]
#         con['relation'] = ll[3]
#         condition.append(con)
#     return condition


@app.route("/model/updateFlow", methods=['POST'])
def update_flow():
    """
    新建 处理流程
    :return:
    """
    userId = request.form.get('userId')
    project_id = request.form.get('projectId')
    config = request.form.get('config')
    start_nodes = request.form.get('startNode')
    relationship = request.form.get('relationship')
    config_order = request.form.get('configOrder')

    print(userId, project_id, config, start_nodes, relationship, config_order)
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
    operator_id = request.form.get('operatorId')

    print(project_id, user_id, operator_id)
    if operator_id is None or operator_id == '':
        flow = ModelService.get_run_status_by_project_id(project_id)
    else:
        flow = ModelService.get_run_status_by_project_id(project_id, operator_id)

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
        _thread.start_new_thread(ModelService.model_execute_from_start, (user_id, project_id))
    except:
        traceback.print_exc()
        print("Error: 无法启动线程")
        return '启动失败'

    return '启动成功'


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
        _thread.start_new_thread(ModelService.model_execute_from_one, (user_id, operator_id))
    except:
        print("Error: 无法启动线程")
        return '启动失败'

    return '启动成功'

# @app.route("/executeAgain", methods=['POST'])
# def executeAgain():
#     """
#     重新执行处理流程（DAG）。
#     请求，判断这个节点的父节点是否执行完成，如果完成 拿父节点输出的数据 作为输入，处理后存储数据并标记该节点已经完成。
#     :return:
#     """
#     projectName = request.form.get('projectName')
#     userId = request.form.get('userId')
#     nodeId = request.form.get('nodeId')  # 节点开始执行的
#     project = getProjectByNameAndUserId(projectName, userId)
#     # print(project)
#     processflow = getProcessFlowByProjectId(project.id)
#     operates = json.loads(processflow.operates)
#     fileUrl = getProjectCurrentDataUrl(projectName)['fileUrl']
#     # print(operates)
#     functionName = projectName + "-executeAgain"
#
#     # spark会话
#     spark = getSparkSession(userId, functionName)
#
#     # 获取数据
#     df = spark.read.format("CSV").option("header", "true").load(fileUrl)
#
#     # 执行DAG图
#     for item in operates:
#         if (item['type'] == '1'):
#             # 解析参数格式
#             condition = parsingFilterParameters(item['operate'])
#             # 过滤函数
#             df = Process.filterCore(spark, df, condition)
#             df.show()
#
#     # 处理后的数据写入文件
#     df.toPandas().to_csv("/home/zk/data/test.csv", header=True)
#     # 返回前50条数据
#     data2 = df.limit(50).toJSON().collect()
#     print(data2)
#     data3 = ",".join(data2)
#     print(data3)
#     data4 = '[' + data3 + ']'
#     print(data4)
#     return jsonify({'length': df.count(), 'data': json.loads(data4)})
