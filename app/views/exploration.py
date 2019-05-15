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
from app.constFile import const

jsonFileName = const.JSONFILENAME

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
        projectName = request.args.get('projectName')
    else:
        projectName = request.form.get('projectName')
    fileUrl = getProjectCurrentDataUrl(projectName)['fileUrl']
    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        return data.columns.values.tolist()
    except:
        return "error read"

# 获取项目对应的当前数据源的 数值型 的列名
@app.route("/getColumnNameWithNumberType", methods=['GET','POST'])
def getColumnNameWithNumberType():
    if request.method == 'GET':
        projectName = request.args.get('projectName')
    else:
        projectName = request.form.get('projectName')
    fileUrl = getProjectCurrentDataUrl(projectName)['fileUrl']
    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        res = []
        for col in data.columns.values.tolist():
            if(data[col].dtype == 'int64' or data[col].dtype == 'float64'):
                res.append(col)
        return res
    except:
        return "error read"

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
    from app.constFile import const

    save_dir = const.SAVEDIR
    # jsonFileName = str(int(time.time()))+'.json'
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
        viewsName = '频次统计'
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
    if (viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif (viewsName == 'FrequencyStatisticsView'):
        viewsName = '频次统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'
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
    if (viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif (viewsName == 'FrequencyStatisticsView'):
        viewsName = '频次统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'
    viewFileUrl = urls['projectAddress']+'/'+viewsName+'/' + jsonFileName
    newfile_path = urls['projectAddress']+'/'+viewsName+'/' + newFileName + '.json'
    # 复制文件
    try:
        shutil.copyfile(viewFileUrl, newfile_path)
        print('保存成功')
        return getfileListFun(viewsName, projectName)
    except Exception as e:
        print('保存失败', e)
        return '保存失败' + str(e)


# 删除某一类视图中的一个视图
@app.route("/deleteViewData", methods=['POST'])
def deleteViewData():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    fileName = request.form.get("fileName")
    print('viewsName: {}, projectName: {}, fileName: {}'.format(viewsName, projectName, fileName))
    urls = getProjectCurrentDataUrl(projectName)
    if (viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif (viewsName == 'FrequencyStatisticsView'):
        viewsName = '频次统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'
    file_path = urls['projectAddress']+'/'+viewsName+'/' + fileName + '.json'
    # 删除文件
    try:
        if(os.path.exists(file_path)):
            shutil.rmtree(file_path)
            print('删除成功')
            return '删除成功'
        else:
            print('文件不存在')
            return '文件不存在'
    except Exception as e:
        print('保存失败', e)
        return '保存失败' + str(e)
'''
# 删除视图
@app.route("/deleteViewData", methods=['POST'])
def deleteViewData():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    jsonFileName = request.form.get("jsonFileName")
    print('viewsName: {}, projectName: {}, jsonFileName: {}'.format(viewsName, projectName, jsonFileName))
    urls = getProjectCurrentDataUrl(projectName)
    if (viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif (viewsName == 'FrequencyStatisticsView'):
        viewsName = '频次统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'

    # 创建垃圾箱, 层级关系：project/垃圾箱/view
    mkdir(urls['projectAddress'] + '/垃圾箱')
    mkdir(urls['projectAddress'] + '/垃圾箱' + '/' + viewsName)

    # 待删除文件列表
    deleteFiles = []
    if jsonFileName == "all":
        for root, dirs, files in os.walk(urls['projectAddress']+'/'+viewsName):
            deleteFiles = files
    else:
        deleteFiles = jsonFileName.split(",")

    # 按照删除列表进行文件移动
    for fileName in deleteFiles:
        if fileName == const.JSONFILENAME:
            continue
        viewFileUrl = urls['projectAddress']+'/'+viewsName+'/' + fileName
        newfile_path = urls['projectAddress'] + '/垃圾箱' + '/' + viewsName + '/' + fileName
        # 移动文件
        try:
            shutil.move(viewFileUrl, newfile_path)
        except Exception as e:
            print('删除失败', e)
            return '删除失败' + str(e)

    print('删除成功')
    return getfileListFun(viewsName, projectName)


# 恢复已删除的视图
@app.route("/restoreViewData", methods=['POST'])
def restoreViewData():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    jsonFileName = request.form.get("jsonFileName")
    print('viewsName: {}, projectName: {}, jsonFileName: {}'.format(viewsName, projectName, jsonFileName))
    urls = getProjectCurrentDataUrl(projectName)
    if (viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif (viewsName == 'FrequencyStatisticsView'):
        viewsName = '频次统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'

    # 待恢复文件列表
    restoreFiles = []
    if jsonFileName == "all":
        for root, dirs, files in os.walk(urls['projectAddress'] + '/垃圾箱' + '/' + viewsName):
            restoreFiles = files
    else:
        restoreFiles = jsonFileName.split(",")

    # 按照恢复列表进行文件移动
    for fileName in restoreFiles:
        if fileName == const.JSONFILENAME:
            continue
        viewFileUrl = urls['projectAddress'] + '/垃圾箱' + '/' + viewsName + '/' + fileName
        newfile_path = urls['projectAddress'] + '/' + viewsName + '/' + fileName
        # 移动文件
        try:
            shutil.move(viewFileUrl, newfile_path)
        except Exception as e:
            print('恢复失败', e)
            return '恢复失败' + str(e)

    print('恢复成功')
    return getfileListFun(viewsName, projectName)


# 获取垃圾箱中文件的列表
@app.route("/getFileListFromWastebin", methods=['POST'])
def getFileListFromWastebin():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    print('viewsName: {}, projectName: {}'.format(viewsName, projectName))
    urls = getProjectCurrentDataUrl(projectName)
    if (viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif (viewsName == 'FrequencyStatisticsView'):
        viewsName = '频次统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'
    viewFileUrl = urls['projectAddress'] + '/垃圾箱' + '/' + viewsName
    # 创建垃圾箱，并将该文件移动至垃圾箱中；层级关系：project/垃圾箱/view
    mkdir(urls['projectAddress'] + '/垃圾箱')
    mkdir(urls['projectAddress'] + '/垃圾箱' + '/' + viewsName)
    # 获取文件列表
    for root, dirs, files in os.walk(viewFileUrl):
        return files


# 彻底删除视图
@app.route("/deleteCompletely", methods=['POST'])
def deleteCompletely():
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    jsonFileName = request.form.get("jsonFileName")
    print('viewsName: {}, projectName: {}, jsonFileName: {}'.format(viewsName, projectName, jsonFileName))
    urls = getProjectCurrentDataUrl(projectName)
    if (viewsName == 'FullTableStatisticsView'):
        viewsName = '全表统计'
    elif (viewsName == 'FrequencyStatisticsView'):
        viewsName = '频次统计'
    elif (viewsName == 'CorrelationCoefficientView'):
        viewsName = '相关系数'
    elif (viewsName == 'ScatterPlot'):
        viewsName = '散点图'

    # 待彻底删除文件列表
    deleteFiles = []
    if jsonFileName == "all":
        for root, dirs, files in os.walk(urls['projectAddress'] + '/垃圾箱' + '/' + viewsName):
            deleteFiles = files
    else:
        deleteFiles = jsonFileName.split(",")

    # 按照删除列表进行彻底删除
    for fileName in deleteFiles:
        if fileName == const.JSONFILENAME:
            continue
        viewFileUrl = urls['projectAddress'] + '/垃圾箱' + '/' + viewsName+'/' + fileName
        # 彻底删除文件
        try:
            os.remove(viewFileUrl)
            # shutil.move(viewFileUrl, newfile_path)
        except Exception as e:
            print('彻底删除失败', e)
            return '彻底删除失败' + str(e)

    print('彻底删除成功')
    # 返回垃圾箱文件列表
    for root, dirs, files in os.walk(urls['projectAddress'] + '/垃圾箱' + '/' + viewsName):
        return files
'''

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
    # 接受请求传参，参数包括：projectName，columnName（只能选一列）
    # 例如：projectName=医药病例分类分析；columnName=Item
    projectName = request.form.get("projectName")
    columnName = request.form.get("columnNames")
    # 报错信息，限选一列
    if len(columnName.split(",")) > 1:
        print(len(columnName.split(",")))
        return "频次统计只能选择一列，请勿多选"
    columnName = columnName.strip("[]")
    columnName = columnName.strip('""')

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
    json_str = json.dumps(res, ensure_ascii=False)
    with open(projectAddress + '/频次统计/' + jsonFileName, "w", encoding="utf-8") as f:
        json.dump(json_str, f, ensure_ascii=False)
        print("加载入文件完成...")

    response = jsonify(res)
    print(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


# 相关系数
@app.route('/correlationCoefficient', methods=['GET', 'POST'])
def correlationCoefficient():
    # 接受请求传参, 参数包括projectName，columnNames（多选，仅限数值型）
    # 例如：projectName=订单分析；columnNames=销售额,折扣,装运成本
    projectName = request.form.get("projectName")
    columnNameStr = request.form.get("columnNames")
    columnNameStr = columnNameStr.strip("[]")
    columnNames = columnNameStr.split(',')
    for i in range(len(columnNames)):
        columnNames[i] = columnNames[i].strip('""')
    print('projectName: {}, columnNames: {}'.format(projectName, columnNames))

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

    # 报错信息：如果所选列不是数值型，则报错
    acceptTypes = ['int64', 'float64']
    for columnName in columnNames:
        if df[columnName].dtype not in acceptTypes:
            return "只能计算数值型列的相关系数，但是 <" + columnName + "> 的类型为 " + str(df[columnName].dtype)

    # 计算出相关系数矩阵df
    df = df.corr()
    res = {}
    print(df)
    # 转存成为dict，此时对数据进行过滤，只显示用户在columnNames里面选择的列
    for index in df.index:
        if index in columnNames:
            temp = {}
            for column in df.columns:
                if column in columnNames:
                    temp.setdefault(column, df.loc[index, column])
            res.setdefault(index, temp)
    print(res)

    # 写入文件
    mkdir(projectAddress + '/相关系数')
    json_str = json.dumps(res, ensure_ascii=False)
    with open(projectAddress + '/相关系数/' + jsonFileName, "w", encoding="utf-8") as f:
        json.dump(json_str, f, ensure_ascii=False)
        print("加载入文件完成...")
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response


# 散点图
@app.route('/scatterPlot', methods=['GET', 'POST'])
def scatterPlot():
    # 接受请求传参，参数包括：projectName，columnNames（必须选两列，数值型）
    # 例如：projectName=订单分析，columnNames=
    projectName = request.form.get("projectName")
    columnNameStr = request.form.get("columnNames")
    columnNameStr = columnNameStr.strip("[]")
    columnNames = columnNameStr.split(',')
    for i in range(len(columnNames)):
        columnNames[i] = columnNames[i].strip('""')
    print('projectName: {}, columnNames: {}'.format(projectName, columnNames))

    # 报错：若选择多于两列，则报错
    if len(columnNames) != 2:
        return "请选择两列，目前的选择为" + str(columnNames)

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
            return "只能画出数值型列的散点图，但是列 <" + columnName + "> 的类型为 " + str(df[columnName].dtype)
    # 写入散点数据
    col1 = columnNames[0]
    col2 = columnNames[1]
    res = {}
    res.setdefault("keys", [col1, col2])
    if len(df) > 50:
        data = df[[col1, col2]].sample(n=50).values.tolist()
    else:
        data = df[[col1, col2]].values.tolist()
    res.setdefault("values", data)

    # 写入文件
    mkdir(projectAddress + '/散点图')
    json_str = json.dumps(res, ensure_ascii=False)
    with open(projectAddress + '/散点图/' + jsonFileName, "w", encoding="utf-8") as f:
        json.dump(json_str, f, ensure_ascii=False)
        print("加载入文件完成...")

    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response
