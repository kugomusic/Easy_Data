# encoding=utf8
import sys
from importlib import reload

reload(sys)

from flask import request, jsonify, Response
from flask.json import jsonify
from app import app
from app import db
from app.models.MSEntity import DataSource, Project, ProcessFlow, initdb
import shutil
import json
import pandas as pd
import os
from app.Utils import mkdir, getProjectCurrentDataUrl, getProjectByNameAndUserId
from app.ConstFile import const
from app.models.ServerNameMap import ServerNameMap


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


# 初始化表，在mysql中新建表，对已存在的表无影响
# initdb()

# # 得到数据源列表
# @app.route('/getDataSource', methods=['GET', 'POST'])
# def getDataSource():
#     DSs = DataSource.query.all()
#     result = []
#     for i in DSs:
#         result.append({"id": i.id, "name": i.file_name})
#     return result


# 创建项目
@app.route('/creatProject', methods=['GET', 'POST'])
def creatProject():
    if request.method == 'GET':
        projectName = request.form.get('projectName')
        dataSourceId = request.form.get('dataSourceId')
        userId = request.form.get('userId')
    else:
        projectName = request.form.get('projectName')
        dataSourceId = request.form.get('dataSourceId')
        userId = request.form.get('userId')
    print('projectName: {}, dataSourceId: {}, userId: {}'.format(projectName, dataSourceId, userId))
    rootUrl = const.ROOTURL
    # 数据库中添加Project记录
    project = Project(project_name=projectName, project_address=rootUrl + projectName, user_id=userId,
                      dataSource_id=dataSourceId)
    db.session.add(project)
    # 数据库中添加ProcessFlow记录
    pro = getProjectByNameAndUserId(projectName, userId)
    processs = ProcessFlow(project_id=pro.id, operates="[]")
    db.session.add(processs)
    db.session.commit()
    try:
        if (not (os.path.exists(rootUrl + projectName))):
            filters = {
                DataSource.id == dataSourceId
            }
            DSs = DataSource.query.filter(*filters).first()
            db.session.commit()
            mkdir(rootUrl + projectName)
            print(DSs.file_url)
            print(rootUrl + projectName)
            shutil.copyfile(DSs.file_url, rootUrl + projectName + '/' + DSs.file_name + '.csv')
            return getProjectList()
        else:
            return "Double name"
    except:
        return "error"


# 获取项目列表
@app.route('/getProjectList', methods=['GET', 'POST'])
def getProjectList():
    DSs = Project.query.all()
    result = []
    for i in DSs:
        result.append({"id": i.id, "name": i.project_name})
    return result


# 获取项目处理流程
@app.route('/getProcessFlowByProjectId', methods=['GET', 'POST'])
def getProcessFlowByProjectId():
    # 接收参数
    if request.method == 'GET':
        projectId = request.args.get("projectId")
    else:
        projectId = request.form.get("projectId")

    # 定义返回状态
    result = {}
    result['success'] = True
    result['errorCode'] = 200
    result['errorMrg'] = ''

    # 查询projectId 对应的处理流程
    try:
        DSs = ProcessFlow.query.filter(ProcessFlow.project_id == projectId).one()
    except:
        result['success'] = False
        result['errorCode'] = 550
        result['errorMrg'] = '没有该项目ID：' + projectId
        return result
    print(DSs)

    # 整理返回格式
    result['id'] = DSs.id
    if (DSs.links is None) or (DSs.links == ''):
        result['linkDataArray'] = ''
    else:
        result['linkDataArray'] = json.loads(DSs.links)

    if (DSs.operates is None) or (DSs.operates == ''):
        operates = []
    else:
        operates = json.loads(DSs.operates)
    for operate in operates:
        operate['operateId'] = operate['type']
        operate['operate'] = json.loads(operate['operate'])
        operate['text'] = ServerNameMap.operateIdToNameMap[operate['operateId']]
        operate['type'] = ServerNameMap.operateIdToTypeMap[operate['operateId']]
        operate['color'] = ServerNameMap.typeToColorMap[operate['type']]
    result['nodeDataArray'] = operates
    result['class'] = 'go.GraphLinksModel'
    return result


# 原始数据预览
@app.route('/rawDataPreview', methods=['GET', 'POST'])
def rawDataPreview():
    if request.method == 'GET':
        start = request.form.get('start')
        end = request.form.get('end')
        projectName = request.form.get('projectName')
    else:
        start = request.form.get('start')
        end = request.form.get('end')
        projectName = request.form.get('projectName')
    print('start: {}, end: {}, projectName: {}'.format(start, end, projectName))
    try:
        urls = getProjectCurrentDataUrl(projectName)
        fileUrl = urls['fileUrl']
    except:
        return "error"
    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        data2 = data[int(start):int(end)].to_json(orient='records', force_ascii=False)
        return jsonify({'length': len(data), 'data': json.loads(data2)})
    except:
        return "error read"


# 当前数据预览
@app.route('/currentDataPreview', methods=['GET', 'POST'])
def currentDataPreview():
    if request.method == 'GET':
        start = request.form.get('start')
        end = request.form.get('end')
        projectName = request.form.get('projectName')
    else:
        start = request.form.get('start')
        end = request.form.get('end')
        projectName = request.form.get('projectName')
    print('start: {}, end: {}, projectName: {}'.format(start, end, projectName))
    try:
        urls = getProjectCurrentDataUrl(projectName)
        # print(urls)
        fileUrl = urls['fileUrl']
        # print(fileUrl)
    except:
        return "error"
    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        data2 = data[int(start):int(end)].to_json(orient='records', force_ascii=False)
        return jsonify({'length': len(data), 'data': json.loads(data2)})
    except:
        return "error read"

