# encoding=utf8
import sys
from importlib import reload
reload(sys)

from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, abort
from flask.json import jsonify
from app import app
from app import db
from app.models.mysql import DataSource, Project,initdb
import shutil
import json
import pandas as pd
import os
from app.utils import mkdir, getProjectCurrentDataUrl


#解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)

app.response_class = MyResponse

#初始化表，在mysql中新建表，对已存在的表无影响
# initdb()

#得到数据源列表
@app.route('/getDataSource', methods=['GET', 'POST'])
def getDataSource():
    DSs = DataSource.query.all()
    result = []
    for i in DSs:
        result.append({"id":i.id,"name":i.file_name})
    return result

#创建项目
@app.route('/creatProject', methods=['GET', 'POST'])
def creatProject():
    if request.method == 'GET':
        projectName = request.args.get('projectName')
        dataSourceId = request.args.get('dataSourceId')
        userId =  request.args.get('userId')
    else:
        projectName = request.form.get('projectName')
        dataSourceId = request.form.get('dataSourceId')
        userId = request.form.get('userId')
    print('projectName: {}, dataSourceId: {}, userId: {}'.format(projectName, dataSourceId,userId))
    rootUrl = '/home/zk/project/'
    # rootUrl = rootUrl
    project = Project(project_name=projectName,project_address=rootUrl+projectName,user_id = userId, dataSource_id = dataSourceId)
    db.session.add(project)

    try:
        if(not (os.path.exists(rootUrl+projectName))):
            filters = {
                DataSource.id ==dataSourceId
            }
            DSs = DataSource.query.filter(*filters).first()
            db.session.commit()
            mkdir(rootUrl+projectName)
            print(DSs.file_url)
            print(rootUrl+projectName)
            shutil.copyfile(DSs.file_url, rootUrl+projectName+'/'+DSs.file_name+'.csv')
            return getProjectList()
        else:
            return "Double name"
    except:
        return "error"

#获取项目列表
@app.route('/getProjectList', methods=['GET','POST'])
def getProjectList():
    DSs = Project.query.all()
    result = []
    for i in DSs:
        result.append({"id": i.id, "name": i.project_name})
    return result

#原始数据预览
@app.route('/rawDataPreview', methods=['GET','POST'])
def rawDataPreview():
    if request.method == 'GET':
        start = request.args.get('start')
        end = request.args.get('end')
        projectName =  request.args.get('projectName')
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

#当前数据预览
@app.route('/currentDataPreview', methods=['GET','POST'])
def currentDataPreview():
    if request.method == 'GET':
        start = request.args.get('start')
        end = request.args.get('end')
        projectName =  request.args.get('projectName')
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