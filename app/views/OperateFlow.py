# -*- coding: UTF-8 -*-
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, \
    abort
from flask.json import jsonify
from app import app
import json
import os
import time
from app.Utils import *
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


# 解析filter参数函数
def parsingFilterParameters(str):
    condition = []
    strList = str.split(';')
    for i in range(len(strList)):
        ll = strList[i].split(',', 3)
        con = {}
        con['name'] = ll[0]
        con['operate'] = ll[1]
        con['value'] = ll[2]
        con['relation'] = ll[3]
        condition.append(con)
    return condition


# 查看处理流程
@app.route("/getOperateFlow", methods=['POST'])
def getOperateFlow():
    projectName = request.form.get('projectName')
    userId = request.form.get('userId')
    project = getProjectByNameAndUserId(projectName, userId)
    # print(project)
    processflow = getProcessFlowByProjectId(project.id)
    operates = json.loads(processflow.operates)
    # print(operates)
    # for item in operates:
    #     # print(item)
    #     # print(item['type'])
    #     # print(item['operate'])
    #     if (item['type'] == '1'):
    #         item['operate'] = parsingFilterParameters(item['operate'])
    print(operates)
    return operates


@app.route("/executeAgain", methods=['POST'])
def executeAgain():
    """
    重新执行处理流程（DAG）。
    请求，判断这个节点的父节点是否执行完成，如果完成 拿父节点输出的数据 作为输入，处理后存储数据并标记该节点已经完成。
    :return:
    """
    projectName = request.form.get('projectName')
    userId = request.form.get('userId')
    nodeId = request.form.get('nodeId') # 节点开始执行的
    project = getProjectByNameAndUserId(projectName, userId)
    # print(project)
    processflow = getProcessFlowByProjectId(project.id)
    operates = json.loads(processflow.operates)
    fileUrl = getProjectCurrentDataUrl(projectName)['fileUrl']
    # print(operates)
    functionName = projectName + "-executeAgain"

    # spark会话
    spark = getSparkSession(userId, functionName)

    # 获取数据
    df = spark.read.format("CSV").option("header", "true").load(fileUrl)

    # 执行DAG图
    for item in operates:
        if (item['type'] == '1'):
            # 解析参数格式
            condition = parsingFilterParameters(item['operate'])
            # 过滤函数
            df = Process.filterCore(spark, df, condition)
            df.show()

    # 处理后的数据写入文件
    df.toPandas().to_csv("/home/zk/data/test.csv", header=True)
    # 返回前50条数据
    data2 = df.limit(50).toJSON().collect()
    print(data2)
    data3 = ",".join(data2)
    print(data3)
    data4 = '[' + data3 + ']'
    print(data4)
    return jsonify({'length': df.count(), 'data': json.loads(data4)})
