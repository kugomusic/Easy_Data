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

# csv文件存储目录（临时）
save_dir = "/home/zk/project/test.csv"
# save_dir = '/Users/kang/PycharmProjects/project/test.csv'

#解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)
app.response_class = MyResponse

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
    for i in range(len(strList)-1):
        ll = strList[i].split(',', 1)
        con ={}
        con['name'] = ll[0]
        con['operate'] = ll[1]
        condition.append(con)
    parameter['condition'] = condition
    return parameter
# print(filterCoreParameter('甜点销售数据预处理', '列名一,关系,值,组合关系;列名一,关系,值,'))

from pyspark.sql import functions as func

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
    for i in range(len(strList) - 1):
        ll = strList[i].split(',', 3)
        con ={}
        con['name'] = ll[0]
        con['operate'] = ll[1]
        con['value'] = ll[2]
        con['newName'] = ll[3]
        condition.append(con)
    parameter['condition'] = condition
    return parameter

from pyspark.sql import functions as func

def columnMapCore(spark, df, condition):
    # val fillColValues = Map("StockCode" -> 5, "Description" -> "No value")
    # df.na.fill(fillColValues)
    for i in condition:
        name = i['name']
        if(i['operate'] == '+'):
            df = df.withColumn(i['newName'], df[name] + i['value'])
        elif(i['operate'] == '-'):
            df = df.withColumn(i['newName'], df[name] - i['value'])
        elif (i['operate'] == '*'):
            df = df.withColumn(i['newName'], df[name] * i['value'])
        elif (i['operate'] == '/'):
            df = df.withColumn(i['newName'], df[name] / i['value'])
    return df
