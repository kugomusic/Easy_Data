# -*- coding: UTF-8 -*-
from flask import request, jsonify, Response
from flask.json import jsonify
from app import app
import json
import os
import time
from app.utils import mkdir, getProjectCurrentDataUrl, is_number, addProcessingFlow
import app.utils as apus
import pandas as pd
from pyspark.sql import SparkSession, utils
from pyspark.sql.functions import split, explode, concat_ws, regexp_replace
from pyspark.ml.feature import Bucketizer, QuantileDiscretizer
from pyspark.mllib.feature import *

# csv文件存储目录（临时）
# save_dir = "/home/zk/project/test.csv"
save_dir = "/Users/tc/Desktop/可视化4.0/Project/test.csv"


# 分位数离散化
@app.route('/quantileDiscretization', methods=['GET', 'POST'])
def quantileDiscretization():
    # 接受请求传参，例如: {"project":"订单分析","columnName":"装运成本","newColumnName":"装运成本(分位数离散化)"}
    requestStr = request.args.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = quantileDiscretizationCore(requestStr)
    if df == "error1":
        return "error: 项目名或项目路径有误"
    elif df == "error2":
        return "error: 只能离散化数值型的列，请检查列名输入是否有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    # df_pandas.to_csv("/Users/tc/Desktop/可视化4.0/Project/test.csv", header=True)
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json()})


# 分位数离散化主函数, 将某列连续型数值进行分箱，加入新列；返回df(spark格式)
def quantileDiscretizationCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['project']
    columnName = requestDict['columnName']
    newColumnName = requestDict['newColumnName']

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error1'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 默认分箱数numBuckets为5，若用户指定，以指定为准
    try:
        numBuckets = int(requestDict['numBuckets'])
    except:
        numBuckets = 5
    # 设定qds（分位数离散化模型）
    qds = QuantileDiscretizer(numBuckets=numBuckets, inputCol=columnName, outputCol=newColumnName,
                              relativeError=0.01, handleInvalid="error")

    # 训练, 输入列必须为数值型，否则返回错误信息
    try:
        df = qds.fit(df).transform(df)
    except utils.IllegalArgumentException:
        return "error2"

    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '8'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df