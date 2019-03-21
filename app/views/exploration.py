# -*- coding: UTF-8 -*-
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, abort
from flask.json import jsonify
from app import app
from app import db
from app.models.mysql import DataSource, Project,initdb
import shutil
import json
import os
import time
from app.utils import mkdir,getProjectCurrentDataUrl
import pandas as pd
from pyspark.sql import SparkSession

#解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)

app.response_class = MyResponse

# 获取项目对应的当前数据源的所有列名
@app.route("/getColumnNames", methods=['GET','POST'])
def getColumnNames():
    if request.method == 'GET':
        projectName =  request.args.get('projectName')
    else:
        projectName = request.form.get('projectName')
    fileUrl = getProjectCurrentDataUrl(projectName)['fileUrl']
    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        return data.columns.values.tolist()
    except:
        return "error read"

jsonFileName = 'qazwsxedcrfvtgbyhnujmiopkl' + '.json'
# 全表统计接口
@app.route("/fullTableStatistics", methods=['POST'])
def fullTableStatistics():
    # 接受参数并处理
    print(request.form)
    columnNameStr = request.form.get('columnNames')
    projectName = request.form.get("projectName")
    columnNameStr = columnNameStr[1:len(columnNameStr)-1]
    columnNames = columnNameStr.split(',')
    for i in range(len(columnNames)):
        columnNames[i] = columnNames[i][1:len(columnNames[i])-1]
    print('projectName: {}, columnNames: {}'.format(projectName, columnNames))
    # 读取项目对应的当前数据
    urls = getProjectCurrentDataUrl(projectName)
    fileUrl = urls['fileUrl']
    projectAddress = urls['projectAddress']
    if fileUrl[-4:] == ".csv":
        df_excel = pd.read_csv(fileUrl, encoding="utf-8")
    else:
        df_excel = pd.read_excel(fileUrl, encoding="utf-8")
    # 全表统计
    res = []
    statistics = [' 字段名',' 类型','总数','最小值','最小值位置','25%分位数','中位数','75%分位数','均值','最大值','最大值位置','平均绝对偏差','方差','标准差','偏度','峰度']
    for columnName in columnNames:
        info = {}.fromkeys(statistics)
        info[' 字段名'] = columnName
        info[' 类型'] = df_excel[columnName].dtype
        if info[' 类型'] == 'int64' or info[' 类型'] == 'float64':
            info[' 类型'] = 'number'
            info['总数'] = str(df_excel[columnName].count())
            info['最小值'] = str(df_excel[columnName].min())
            info['最小值位置'] = str(df_excel[columnName].idxmin())
            info['25%分位数'] = str(df_excel[columnName].quantile(.25))
            info['中位数'] = str(df_excel[columnName].median())
            info['75%分位数'] = str(df_excel[columnName].quantile(.75))
            info['均值'] = str(df_excel[columnName].mean())
            info['最大值'] = str(df_excel[columnName].max())
            info['最大值位置'] = str(df_excel[columnName].idxmax())
            info['平均绝对偏差'] = str(df_excel[columnName].mad())
            info['方差'] = str(df_excel[columnName].var())
            info['标准差'] = str(df_excel[columnName].std())
            info['偏度'] = str(df_excel[columnName].skew())
            info['峰度'] = str(df_excel[columnName].kurt())
            print('int')
        else:
            info[' 类型'] = "text"
            info['总数'] = str(df_excel[columnName].count())
            print("text")
        res.append(info)
    # 写入文件
    mkdir(projectAddress+'/全表统计')
    # jsonFileName = str(int(time.time()))+'.json'
    # jsonFileName = 'qazwsxedcrfvtgbyhnujmiopkl' + '.json'
    json_str = json.dumps(res, ensure_ascii=False)
    with open(projectAddress+'/全表统计/' + jsonFileName, "w", encoding="utf-8") as f:
        json.dump(json_str, f, ensure_ascii=False)
        print("加载入文件完成...")
    result = {}
    result['fileName'] = jsonFileName
    result['data'] = res
    response = jsonify(result)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

def getfileListFun(viewsName,projectName):
    urls = getProjectCurrentDataUrl(projectName)
    if(viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif(viewsName == 'FrequencyStatisticsView'):
        viewsName = '频率统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'
    projectAddress = urls['projectAddress']+'/'+viewsName
    if not os.path.exists(projectAddress):
        return []
    for root, dirs, files in os.walk(projectAddress):
        # print(root) #当前目录路径
        # print(dirs) #当前路径下所有子目录
        print(files)  #当前路径下所有非目录子文件！
    result = []
    for i in range(len(files)):
        if files[i] != jsonFileName:
            le = len(files[i])
            result.append(files[i][0:le - 5])
    return result

# 获取某一类视图的列表(核心函数是 getfileListFun)
@app.route("/getfileList", methods=['POST'])
def getfileList():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    print('viewsName: {}, projectName: {}'.format(viewsName, projectName))
    return getfileListFun(viewsName, projectName)

# 获取某一类视图中的一个视图
@app.route("/getViewData", methods=['POST'])
def getViewData():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    viewFileName = request.form.get("viewFileName")
    print('viewsName: {}, projectName: {}, viewFileName: {}'.format(viewsName, projectName, viewFileName))
    urls = getProjectCurrentDataUrl(projectName)
    if(viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    viewFileUrl = urls['projectAddress']+'/'+viewsName+'/' + viewFileName +'.json'
    # 读入数据
    with open(viewFileUrl, 'r') as load_f:
        load_dict = json.load(load_f)
        load_dict = json.loads(load_dict)
    return load_dict

# 保存某一类视图中的一个视图
@app.route("/saveViewData", methods=['POST'])
def saveViewData():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    newFileName = request.form.get("newFileName")
    print('viewsName: {}, projectName: {}, newFileName: {}'.format(viewsName, projectName, newFileName))
    urls = getProjectCurrentDataUrl(projectName)
    if(viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    viewFileUrl = urls['projectAddress']+'/'+viewsName+'/' + jsonFileName
    newfile_path = urls['projectAddress']+'/'+viewsName+'/' + newFileName + '.json'
    # 复制文件
    try:
        shutil.copyfile(viewFileUrl, newfile_path)
        print('保存成功')
        return getfileListFun(viewsName, projectName)
    except:
        print('保存失败')
        return '保存失败'


# 获取项目对应的当前数据源的所有列名和所有视图（）
@app.route("/getColumnNamesAndViews", methods=['GET','POST'])
def getColumnNamesAndViews():
    if request.method == 'GET':
        projectName =  request.args.get('projectName')
    else:
        projectName = request.form.get('projectName')
    fileUrl = getProjectCurrentDataUrl(projectName)['fileUrl']
    result = {}
    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        result['columnNames'] = data.columns.values.tolist()
        result['FullTableStatisticsView'] = getfileListFun('FullTableStatisticsView', projectName)
        result['FrequencyStatisticsView'] = getfileListFun('FrequencyStatisticsView', projectName)
        result['CorrelationCoefficientView'] = getfileListFun('CorrelationCoefficientView', projectName)
        result['ScatterPlot'] = getfileListFun('ScatterPlot', projectName)
        return result
    except:
        return "error read"


# 频次统计
@app.route('/frequencyStatistics', methods=['GET', 'POST'])
def frequencyStatistics():
    # 接受请求传参，参数格式为json->字典, 例如: {"project":"医药病例分类分析","columnName":"Item"}
    requestStr = request.args.get("requestStr")
    requestDict = json.loads(requestStr)
    projectName = requestDict['project']
    columnName = requestDict['columnName']

    # 读取项目对应的当前数据
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return '项目名或项目路径有误'
    fileUrl = urls['fileUrl']
    projectAddress = urls['projectAddress']
    if fileUrl[-4:] == ".csv":
        df = pd.read_csv(fileUrl, encoding="utf-8")
    else:
        df = pd.read_excel(fileUrl, encoding="utf-8")

    # 频次统计
    res = {}
    for i in range(len(df[columnName].value_counts().index)):
        res.setdefault(df[columnName].value_counts().index[i], str(df[columnName].value_counts().values[i]))
        i += 1

    # 写入文件
    mkdir(projectAddress + '/频次统计')
    jsonFileName = str(int(time.time())) + '.json'
    json_str = json.dumps(res)
    with open(projectAddress + '/频次统计/' + jsonFileName, "w") as f:
        json.dump(json_str, f)
        print("存储json文件完成...")
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


# 相关系数
@app.route('/correlationCoefficient', methods=['GET', 'POST'])
def correlationCoefficient():
    # 接受请求传参, projectName = "订单分析"
    projectName = request.args.get("project")

    # 读取项目对应的当前数据
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return '项目名或项目路径有误'
    fileUrl = urls['fileUrl']
    projectAddress = urls['projectAddress']
    if fileUrl[-4:] == ".csv":
        df = pd.read_csv(fileUrl, encoding="utf-8")
    else:
        df = pd.read_excel(fileUrl, encoding="utf-8")

    # 计算出相关系数矩阵df
    df = df.corr()
    res = {}
    for index in df.index:
        temp = {}
        for column in df.columns:
            temp.setdefault(column, df.loc[index, column])
        res.setdefault(index, temp)

    # 写入文件
    mkdir(projectAddress + '/相关系数')
    jsonFileName = str(int(time.time())) + '.json'
    json_str = json.dumps(res)
    with open(projectAddress + '/相关系数/' + jsonFileName, "w") as f:
        json.dump(json_str, f)
        print("存储json文件完成...")
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


# 散点图
@app.route('/scatterPlot', methods=['GET', 'POST'])
def scatterPlot():
    # 接受请求传参，参数格式为json->字典, 例如: {"project":"订单分析","columnNames":"['销售额', '数量', '折扣', '利润', '装运成本']"}
    requestStr = request.args.get("requestStr")
    requestDict = json.loads(requestStr)
    projectName = requestDict['project']
    columnNamesStr = requestDict['columnNames'].strip('[]')
    columnNames = []
    temp = columnNamesStr.split(',')
    for column in temp:
        column = column.strip()
        column = column.strip("''")
        columnNames.append(column)

    # 读取项目对应的当前数据
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return '项目名或项目路径有误'
    fileUrl = urls['fileUrl']
    projectAddress = urls['projectAddress']
    if fileUrl[-4:] == ".csv":
        df = pd.read_csv(fileUrl, encoding="utf-8")
    else:
        df = pd.read_excel(fileUrl, encoding="utf-8")

    # 判断所选列的数据类型是否为“数字型”，若不符，返回错误信息
    acceptTypes = ['int64', 'float64']
    for columnName in columnNames:
        if df[columnName].dtype not in acceptTypes:
            return "Can only draw Scatter plot with numeric fields, but " + columnName + " is " + str(df[columnName].dtype)
    # 写入散点数据
    res = {}
    for col1 in columnNames:
        for col2 in columnNames:
            combineKey = col1 + ' + ' + col2
            if col1 == col2:
                res.setdefault(combineKey, 'equal')             # 同列是否需要数据画出散点图？
            else:
                res.setdefault(combineKey, df[[col1, col2]].values.tolist())

    # 写入文件
    mkdir(projectAddress + '/散点图')
    jsonFileName = str(int(time.time())) + '.json'
    json_str = json.dumps(res)
    with open(projectAddress + '/散点图/' + jsonFileName, "w") as f:
        json.dump(json_str, f)
        print("存储json文件完成...")
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
