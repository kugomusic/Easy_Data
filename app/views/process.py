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

# 欢迎页面
@app.route("/", methods=['GET', 'POST'])
def hello():
    return "<h1 style='color:blueviolet'> HomePage of Easy_Data</h1>"


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
    df.show()
    # 过滤函数
    sqlDF = filterCore(spark, df, condition)
    sqlDF.show()
    # 处理后的数据写入文件
    # sqlDF.write.csv(path='/home/zk/data/test.csv', header=True, sep=",", mode="overwrite")
    # sqlDF.coalesce(1).write.option("header", "true").csv("/home/zk/data/test.csv")
    sqlDF.toPandas().to_csv(save_dir, header=True)
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
    strList = parameterStr[0:len(parameterStr)-1].split(';')
    for i in range(len(strList) - 1):
        ll = strList[i].split(',', 3)
        con ={}
        con['name'] = ll[0]
        con['operate'] = ll[1]
        con['value'] = ll[2]
        con['relation'] = ll[3]
        condition.append(con)
    ll = strList[len(strList) - 1].split(',', 2)
    con = {}
    con['name'] = ll[0]
    con['operate'] = ll[1]
    con['value'] = ll[2]
    con['relation'] = ""
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
    print(sqlStr)
    df.createOrReplaceTempView(tableName)
    sqlDF = spark.sql(sqlStr)

    return sqlDF


# 排序 页面路由
@app.route("/sort", methods=['GET', 'POST'])
def sort():
    # 接受请求传参，例如: {"projectName":"订单分析","columnName":"利润","sortType":"降序"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = sortCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行排序"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    print(df_pandas)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 排序主函数，函数功能包括解析参数、排序；返回df(spark格式)
def sortCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # sortType默认为升序，若用户指定，以用户指定为准
    try:
        sortType = requestDict['sortType']
    except:
        sortType = "升序"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'                                 # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 排序
    if sortType == "降序":
        print("按降序排列")
        df = df.sort(columnName, ascending=False)
    else:
        print("按升序排列")
        df = df.sort(columnName)

    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '2'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 按列拆分页面路由
@app.route("/columnSplit", methods=['GET', 'POST'])
def columnSplit():
    # 接受请求传参，例如: {"project":"订单分析","columnName":"订购日期","newColumnNames":["年","月","日"]}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = columnSplitCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行拆分"
    elif df == "error_splitSymbol":
        return "error: 您指定的列中无可供拆分的符号"
    elif df == "error_splitNum":
        return "error：您指定的拆分列数与目标列不匹配，请重新输入"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 按列拆分主函数，函数功能包括解析参数、拆分；返回df(spark格式)
# 自动识别拆分目标列中的符号，如：2019/03/25中的"/"
def columnSplitCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # 获取拆分出的新列的列名，若未指定，暂时存储为空列表，后续根据拆分数填充成为[拆分列1,拆分列2,拆分列3,...]
    try:
        newColumnNames = requestDict['newColumnNames']
    except:
        newColumnNames = []

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    sc = spark.sparkContext

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'                                 # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 拆分
    first_row = df.first()
    splitStr = first_row[columnName]
    # 识别splitStr中的符号
    splitSymbol = symbolRecognition(splitStr)
    if splitSymbol == '':
        return "error_splitSymbol"                                 # 错误类型：指定列中不含可供拆分的符号

    # 将指定列columnName按splitSymbol拆分，存入"splitColumn"列，列内数据格式为[a, b, c, ...]
    df_split = df.withColumn("splitColumn", split(df[columnName], splitSymbol))
    splitNumber = len(first_row[columnName].split(splitSymbol))
    # 若用户为指定拆分出的新列的列名，根据拆分数填充
    if newColumnNames == []:
        for i in range(splitNumber):
            newColumnNames.append("拆分列" + str(i + 1))
    print(newColumnNames)
    # 若用户已指定拆分出的新列的列名，检查其是否与拆分数一致，若不一致，报错
    if splitNumber != len(newColumnNames):
        return "error_splitNum"                                 # 错误类型：指定列拆分后数量与新的列名数不一致
    # 给新列名生成索引，格式为：[('年', 0), ('月', 1), ('日', 2)]，方便后续操作
    newColumnNames_with_index = sc.parallelize(newColumnNames).zipWithIndex().collect()
    # 遍历生成新列
    for name, index in newColumnNames_with_index:
        df_split = df_split.withColumn(name, df_split["splitColumn"].getItem(index))
    df = df_split.drop("splitColumn")
    df.show()

    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '4.1'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 识别符号
def symbolRecognition(splitStr):
    splitSymbol = ''
    symbolList = ['/', '-', '_', '#', '@', '!', '$', '|', '=', ',', '.', '*']
    for i in list(splitStr):
        if i in symbolList:
            splitSymbol = i
            break
    return splitSymbol


# 按行拆分页面路由
@app.route("/rowSplit", methods=['GET', 'POST'])
def rowSplit():
    # 接受请求传参，例如: {"project":"订单分析","columnName":"订单ID","newColumnName":"订单ID分割"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = rowSplitCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行拆分"
    elif df == "error_splitSymbol":
        return "error: 您指定的列中无可供拆分的符号"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 按行拆分主函数，函数功能包括解析参数、拆分；返回df(spark格式)
# 自动识别拆分目标列中的符号，如：2019/03/25中的"/"
def rowSplitCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # newColumnName默认为columnName+“拆分”，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = columnName + "拆分"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'                                 # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 拆分
    first_row = df.first()
    splitStr = first_row[columnName]
    # 识别splitStr中的符号
    splitSymbol = symbolRecognition(splitStr)
    if splitSymbol == '':
        return "error_splitSymbol"                                 # 错误类型：指定列中不含可供拆分的符号

    # 将指定列columnName按splitSymbol拆分，存入newColumnName列的多行
    df = df.withColumn(newColumnName, explode(split(df[columnName], splitSymbol)))
    df.show()

    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '4.2'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 多列合并页面路由
@app.route("/columnsMerge", methods=['GET', 'POST'])
def columnsMerge():
    # 接受请求传参，例如: {"projectName":"订单分析","columnNames":["类别","子类别","产品名称"],"newColumnName":"品类名称","splitSymbol":"-"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = columnsMergeCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 多列合并主函数，新增一列，列内的值为指定多列合并而成；返回df(spark格式)
def columnsMergeCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnNames = requestDict['columnNames']
    print(columnNames)
    print(type(columnNames))
    # 默认分隔符是","，若requestStr中指定了分隔符，则以用户指定为准
    try:
        splitSymbol = requestDict['splitSymbol']
    except:
        splitSymbol = ','
    # 默认新列名称为：合并结果(col1, col2, col3, ...)，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = "合并结果" + "(" + str(columnNames).strip("[]") + ")"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'                                 # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 合并(spark的dataframe操作好蠢，暂时先用笨办法合并吧 >_< )
    if len(columnNames) == 2:
        df = df.withColumn(newColumnName, concat_ws(splitSymbol, df[columnNames[0]], df[columnNames[1]]))
    elif len(columnNames) == 3:
        df = df.withColumn(newColumnName, concat_ws(splitSymbol, df[columnNames[0]], df[columnNames[1]], df[columnNames[2]]))
    elif len(columnNames) == 4:
        df = df.withColumn(newColumnName, concat_ws(splitSymbol, df[columnNames[0]], df[columnNames[1]], df[columnNames[2]], df[columnNames[3]]))

    df.show()

    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '4.3'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 数据列替换 页面路由
@app.route("/replace", methods=['GET', 'POST'])
def replace():
    # 接受请求传参，例如: {"project":"订单分析","columnName":"客户ID","sourceCharacter":"0","targetCharacter":"Q"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = replaceCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行替换"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 数据列替换主函数, 将某列中的字符进行替换；返回df(spark格式)
def replaceCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    sourceCharacter = requestDict['sourceCharacter']
    targetCharacter = requestDict['targetCharacter']

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'                                 # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 字符替换
    df = df.withColumn("temp", (regexp_replace(df[columnName], sourceCharacter, targetCharacter)))
    df = df.drop(columnName)
    df = df.withColumnRenamed("temp", columnName)
    df.show()

    #追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '6'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df