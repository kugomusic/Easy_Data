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
    jsonFileName = str(int(time.time()))+'.json'
    json_str = json.dumps(res)
    with open(projectAddress+'/全表统计/' + jsonFileName, "w") as f:
        json.dump(json_str, f)
        print("加载入文件完成...")
    response = jsonify(res)
    response.headers.add('Access-Control-Allow-Origin', '*')
    return response

# 等待完成 （获取某一类视图的列表）
def getfileList(file_dir):
    viewsName = request.form.get("viewsName")
    projectName = request.form.get("projectName")
    for root, dirs, files in os.walk(file_dir):
        print(root) #当前目录路径
        print(dirs) #当前路径下所有子目录
        print(files) #当前路径下所有非目录子文件！


# fullTableStatistics()
# fileUrl = '/Users/kang/PycharmProjects/project/爱德信息分析项目/订单信息全量.csv'
# @app.route("/table_analysis")
# def tabanalysis():
#     # table_name = request.args.get("tabname")
#     res = {}
#     if fileUrl[-4:] == ".csv":
#         df_excel = pd.read_csv(fileUrl, encoding="utf-8")
#     else:
#         df_excel = pd.read_excel(fileUrl, encoding="utf-8")
#
#     for value in df_excel.columns.values.tolist():
#         res[value] = {}
#         res[value]["data"] = {}
#         res[value]["property"] = {}
#         list = df_excel[df_excel[value].notnull()][value].drop_duplicates().tolist()
#
#         # print(list)
#         if list:
#             if len(list) == 2:  # flag
#                 res[value].setdefault("type", "flag")
#                 df = df_excel[value].value_counts()
#                 dff = df.to_dict()
#                 for key in dff:
#                     print(dff[key])
#                     res[value]["data"].setdefault(key, int(str(dff[key])))
#                 print(type(df))
#                 res[value]["property"].setdefault("valid", int(str(df_excel[value].count())))
#             elif isinstance(list[0], (int, float)):  # range
#                 res[value].setdefault("type", "range")
#                 df = df_excel[value].describe()
#                 var = df_excel[value].var()
#                 std = df_excel[value].std()
#                 max = df[7]
#                 min = df[3]
#                 #inc = (max - min) / 5
#                 #i = 1
#                 #ff = min
#                 #while min < max:
#                 #    temp = int(ff + inc * i)
#                 #    if temp == max:
#                 #        num = len(df_excel[(df_excel[value] >= min) & (df_excel[value] <= max)])
#                 #    else:
#                 #        num = len(df_excel[(df_excel[value] >= min) & (df_excel[value] < temp)])
#                 #    res[value]["data"].setdefault('{m}-{t}'.format(m=min, t=temp), num)
#                 #    min = temp
#                 #    i = i + 1
#                 res[value]["property"].setdefault('max', round(max, 2))
#                 res[value]["property"].setdefault('25%', round(df[4], 2))
#                 res[value]["property"].setdefault('75%', round(df[6], 2))
#                 res[value]["property"].setdefault('min', round(min, 2))
#                 res[value]["property"].setdefault('mean', round(df[1], 2))
#                 res[value]["property"].setdefault('median', round(df[5], 2))
#                 res[value]["property"].setdefault('valid', round(df[0], 2))
#                 res[value]["property"].setdefault('var', round(var, 2))
#                 res[value]["property"].setdefault('std',round(std,2))
#                 res[value]["property"].setdefault('iqr',round(df[6]-df[4],2))
#             else:
#                 res[value].setdefault("type", "set")
#                 # for col in list:
#                     # num = len(df_excel[df_excel[value] == col])
#                     # res[value]["data"].setdefault(col, num)
#                 df = df_excel[value].value_counts()
#                 dff = df.to_dict()
#                 for key in dff:
#                     # print(dff[key])
#                     res[value]["data"].setdefault(key, int(str(dff[key])))
#                 # print(type(df))
#                 res[value]["property"].setdefault('valid', int(str(df_excel[value].count())))
#         else:
#             res[value].setdefault("type", "null")
#     response = jsonify(res)
#     response.headers.add('Access-Control-Allow-Origin', '*')
#     return response
# tabanalysis()


# spark = SparkSession \
#     .builder \
#     .appName('my_first_app_name') \
#     .getOrCreate()
#
# # 从pandas dataframe创建spark dataframe
# colors = ['white','green','yellow','red','brown','pink']
# color_df=pd.DataFrame(colors,columns=['color'])
# color_df['length']=color_df['color'].apply(len)
#
# color_df=spark.createDataFrame(color_df)
# color_df.show()
#
# # 查看列的类型 ，同pandas
# color_df.dtypes
#
# # [('color', 'string'), ('length', 'bigint')]
#
# # 行数
# color_df.count()
#
# # 如果是pandas
# len(color_df)