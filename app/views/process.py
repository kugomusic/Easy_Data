# -*- coding: UTF-8 -*-
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, abort
from flask.json import jsonify
from app import app
import json
import os
import time
from app.utils import mkdir,getProjectCurrentDataUrl,is_number,addProcessingFlow
import app.utils as apus
import pandas as pd
from pyspark.sql import SparkSession
import random
import string

#解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)

app.response_class = MyResponse

@app.route("/filter", methods=['GET','POST'])
def filterMultiConditions():
    projectName = request.form.get('projectName')
    userId = request.form.get('userId')
    parameterStr = request.form.get('parameter')
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    #测试用例
    # parameter = {}
    # parameter['fileUrl'] = '/home/zk/data/订单信息.csv'
    # condition = []
    # con1 = {'name':'利润','operate':'>','value':'100','relation':'AND'}
    # con2 = {'name':'装运方式','operate':'==','value':'一级','relation':''}
    # condition.append(con1)
    # condition.append(con2)
    # parameter['condition'] = condition
    # 解析参数格式
    parameter = filterCoreParameter(projectName, parameterStr)
    fileUrl = parameter['fileUrl']
    condition = parameter['condition']
    df = spark.read.format("CSV").option("header", "true").load(fileUrl)
    # 过滤函数
    sqlDF = filterCore(spark, df, condition)
    sqlDF.show()
    # 处理后的数据写入文件
    # sqlDF.write.csv(path='/home/zk/data/test.csv', header=True, sep=",", mode="overwrite")
    # sqlDF.coalesce(1).write.option("header", "true").csv("/home/zk/data/test.csv")
    sqlDF.toPandas().to_csv("/home/zk/data/test.csv", header=True)
    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '1'
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

def filterCoreParameter(projectName, parameterStr):
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
        ll = strList[i].split(',', 3)
        con ={}
        con['name'] = ll[0]
        con['operate'] = ll[1]
        con['value'] = ll[2]
        con['relation'] = ll[3]
        condition.append(con)
    parameter['condition'] = condition
    return parameter
# print(filterCoreParameter('甜点销售数据预处理', '列名一,关系,值,组合关系;列名一,关系,值,'))

def filterCore(spark, df, condition):
    tableName = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    sqlStr = 'select * from '+tableName +' where'
    # types = {}
    # for i in df.dtypes:
    #     types[i[0]] = i[1]
    #     print(i)
    for i in condition:
        if is_number(i['value']):
            sqlStr = sqlStr + ' `' + i['name'] + '` ' + i['operate'] + ' ' + i['value'] + ' ' + i['relation']
        else:
            sqlStr = sqlStr + ' `' + i['name'] + '` ' + i['operate'] + ' \"' + i['value'] + '\" ' + i['relation']
    df.createOrReplaceTempView(tableName)
    sqlDF = spark.sql(sqlStr)
    return sqlDF
