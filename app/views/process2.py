# -*- coding: UTF-8 -*-
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, abort
from flask.json import jsonify
from app import app
import json
import os
import time
from app.utils import mkdir, getProjectCurrentDataUrl, is_number, addProcessingFlow
import app.utils as apus
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, concat_ws, regexp_replace
import random
import string
from app.constFile import const

save_dir = const.SAVEDIR

#解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)
app.response_class = MyResponse

# 填充空值
@app.route("/fillNullValue", methods=['GET','POST'])
def fillNullValue():
    projectName = request.form.get('projectName')
    userId = request.form.get('userId')
    parameterStr = request.form.get('parameter')
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # 解析参数格式
    parameter = fillNullValueParameter(projectName, parameterStr)
    fileUrl = parameter['fileUrl']
    condition = parameter['condition']
    df = spark.read.format("CSV").option("header", "true").load(fileUrl)
    # 过滤函数
    sqlDF = fillNullValueCore(spark, df, condition)
    sqlDF.show()
    # 处理后的数据写入文件
    sqlDF.toPandas().to_csv(save_dir, header=True)
    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '3'
    operateParameter['operate'] = parameterStr
    addProcessingFlow(projectName, userId, operateParameter)
    # 返回前50条数据
    data2 = sqlDF.limit(50).toJSON().collect()
    print(data2)
    data3 = ",".join(data2)
    print(data3)
    data4 = '['+data3+']'
    print(data4)
    return jsonify({'length': sqlDF.count(), 'data': json.loads(data4)})

def fillNullValueParameter(projectName, parameterStr):
    try:
        urls = getProjectCurrentDataUrl(projectName)
        fileUrl = urls['fileUrl']
    except:
        return "error"
    parameter = {}
    parameter['fileUrl'] = fileUrl
    condition = []
    strList = parameterStr.split(';')
    for i in range(len(strList)):
        if strList[i] == "" or strList[i] == None:
            continue
        ll = strList[i].split(',', 1)
        con ={}
        con['name'] = ll[0]
        con['operate'] = ll[1]
        condition.append(con)
    parameter['condition'] = condition
    return parameter
# print(fillNullValueParameter('特征工程测试项目', '列名一,填充方法;列名一,填充方法'))

def fillNullValueCore(spark, df, condition):
    # val fillColValues = Map("StockCode" -> 5, "Description" -> "No value")
    # df.na.fill(fillColValues)
    for i in condition:
        if(i['operate'] == '均值填充'):
            mean_item = df.select(func.mean(i['name'])).collect()[0][0]
            df = df.na.fill({i['name']: mean_item})
        elif(i['operate'] == '最大值填充'):
            mean_item = df.select(func.max(i['name'])).collect()[0][0]
            df = df.na.fill({i['name']: mean_item})
        elif (i['operate'] == '最小值填充'):
            mean_item = df.select(func.min(i['name'])).collect()[0][0]
            df = df.na.fill({i['name']: mean_item})
    return df

@app.route("/columnMap", methods=['GET','POST'])
def columnMap():
    projectName = request.form.get('projectName')
    userId = request.form.get('userId')
    parameterStr = request.form.get('parameter')
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    # 解析参数格式
    parameter = columnMapParameter(projectName, parameterStr)
    fileUrl = parameter['fileUrl']
    condition = parameter['condition']
    df = spark.read.format("CSV").option("header", "true").load(fileUrl)
    # 过滤函数
    sqlDF = columnMapCore(spark, df, condition)
    sqlDF.show()
    # 处理后的数据写入文件
    sqlDF.toPandas().to_csv(save_dir, header=True)
    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '3'
    operateParameter['operate'] = parameterStr
    addProcessingFlow(projectName, userId, operateParameter)
    # 返回前50条数据
    data2 = sqlDF.limit(50).toJSON().collect()
    print(data2)
    data3 = ",".join(data2)
    print(data3)
    data4 = '['+data3+']'
    print(data4)
    return jsonify({'length': sqlDF.count(), 'data': json.loads(data4)})

def columnMapParameter(projectName, parameterStr):
    try:
        urls = getProjectCurrentDataUrl(projectName)
        fileUrl = urls['fileUrl']
    except:
        return "error"
    parameter = {}
    parameter['fileUrl'] = fileUrl
    condition = []
    strList = parameterStr.split(';')
    for i in range(len(strList)):
        if strList[i] == "" or strList[i] == None:
            continue
        ll = strList[i].split(',', 7)
        con ={}
        con['name1'] = ll[0]
        con['operate1'] = ll[1]
        con['value1'] = ll[2]

        con['operate'] = ll[3]
        con['name2'] = ll[4]
        con['operate2'] = ll[5]
        con['value2'] = ll[6]

        con['newName'] = ll[7]
        condition.append(con)
    parameter['condition'] = condition
    return parameter

from pyspark.sql import functions as func

def columnMapCore(spark, df, condition):
    # val fillColValues = Map("StockCode" -> 5, "Description" -> "No value")
    # df.na.fill(fillColValues)
    for i in condition:
        name1 = i['name1']
        name2 = i['name2']
        newName = i['newName']
        if (i['operate1'] == '+'):
            df = df.withColumn(newName, df[name1] + i['value1'])
        elif (i['operate1'] == '-'):
            df = df.withColumn(newName, df[name1] - i['value1'])
        elif (i['operate1'] == '*'):
            df = df.withColumn(newName, df[name1] * i['value1'])
        elif (i['operate1'] == '/'):
            df = df.withColumn(newName, df[name1] / i['value1'])
        if(not ((name2 == "") or (name2 == None ))):
            newName2 = newName+"_2"
            if (i['operate2'] == '+'):
                df = df.withColumn(newName2, df[name2] + i['value2'])
            elif (i['operate2'] == '-'):
                df = df.withColumn(newName2, df[name2] - i['value2'])
            elif (i['operate2'] == '*'):
                df = df.withColumn(newName2, df[name2] * i['value2'])
            elif (i['operate2'] == '/'):
                df = df.withColumn(newName2, df[name2] / i['value2'])

            if (i['operate'] == '+'):
                df = df.withColumn(newName, df[newName] + df[newName2])
            elif (i['operate'] == '-'):
                df = df.withColumn(newName, df[newName] - df[newName2])
            elif (i['operate'] == '*'):
                df = df.withColumn(newName, df[newName] * df[newName2])
            elif (i['operate'] == '/'):
                df = df.withColumn(newName, df[newName] / df[newName2])
            df = df.drop(newName2)
    return df
