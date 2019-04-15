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
from pyspark.ml.feature import *
from app.constFile import const

save_dir = const.SAVEDIR

# 分位数离散化页面路由
@app.route('/quantileDiscretization', methods=['GET', 'POST'])
def quantileDiscretization():
    # 接受请求传参，例如: {"projectName":"订单分析","columnName":"装运成本","newColumnName":"装运成本(分位数离散化)","numBuckets":10}
    # 参数中可指定分箱数numBuckets, 默认为5
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = quantileDiscretizationCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行分位数离散化"
    elif df == "error_numerical":
        return "error: 只能离散化数值型的列，请检查列名输入是否有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 分位数离散化主函数, 将某列连续型数值进行分箱，加入新列；返回df(spark格式)
def quantileDiscretizationCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # 新列的列名默认为columnName + "(分位数离散化)"，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = columnName + "(分位数离散化)"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
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
        return "error_numerical"

    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '8'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 向量索引转换旨在转换Vector, 例如：[aa, bb, cc]，而非本例中的单独值，由于没有合适的数据可用，暂时把单独值转换成vector实现功能: aa -> [aa]
# 向量索引转换页面路由
@app.route('/vectorIndexer', methods=['GET', 'POST'])
def vectorIndexer():
    # 接受请求传参，例如: {"projectName":"订单分析","columnName":"装运成本","newColumnName":"向量索引转换结果","maxCategories":50}
    # 参数中指定分类标准maxCategories, 默认为20（maxCategories是一个阈值，如[1.0, 2.0, 2.5]的categories是3，小于20，属于分类类型；否则为连续类型）
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = vectorIndexerCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行向量索引转化"
    elif df == "error_numerical":
        return "error: 只能转化数值型的列，请检查列名输入是否有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 向量索引转换主函数, 将向量转换成标签索引格式，加入新列；返回df(spark格式)
def vectorIndexerCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # 新列的列名默认为columnName + "(向量索引转换)"，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = columnName + "(向量索引转换)"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 默认分类阈值maxCategories为20，若用户指定，以指定为准
    try:
        maxCategories = int(requestDict['maxCategories'])
    except:
        maxCategories = 20

    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    vecAssembler = VectorAssembler(inputCols=[columnName], outputCol="features")
    try:
        df = vecAssembler.transform(df)
    except utils.IllegalArgumentException:
        return "error_numerical"

    # 定义indexer（向量索引转换模型）
    indexer = VectorIndexer(maxCategories=maxCategories, inputCol="features", outputCol=newColumnName)

    # 训练
    df = indexer.fit(df).transform(df)

    df = df.drop("features")
    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '10'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 标准化列页面路由
@app.route('/standardScaler', methods=['GET', 'POST'])
def standardScaler():
    # 接受请求传参，例如: {"projectName":"订单分析","columnName":"利润","newColumnName":"利润(标准化)"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = standardScalerCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行标准化"
    elif df == "error_numerical":
        return "error: 只能标准化数值型的列，请检查列名输入是否有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 标准化列主函数, 标准化列，使其拥有零均值和等于1的标准差；返回df(spark格式)
def standardScalerCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # 新列的列名默认为columnName + "(向量索引转换)"，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = columnName + "(标准化)"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    vecAssembler = VectorAssembler(inputCols=[columnName], outputCol="features")
    try:
        df = vecAssembler.transform(df)
    except utils.IllegalArgumentException:
        return "error_numerical"

    # 设定标准化模型
    standardScaler = StandardScaler(inputCol="features", outputCol=newColumnName)

    # 训练
    df = standardScaler.fit(df).transform(df)

    df = df.drop("features")
    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '12'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 降维页面路由
@app.route('/pca', methods=['GET', 'POST'])
def pca():
    # 接受请求传参，例如: {"projectName":"订单分析","columnNames":["销售额","数量","折扣","利润","装运成本"],"newColumnName":"降维结果","k":4}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = pcaCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_numerical":
        return "error: 只能降维数值型的列，请检查列名输入是否有误"
    elif df == "error_targetDimensions":
        return "error: 目标维度k必须小于原始维度，才能完成降维，请检查输入的列名和目标维度k（k默认为3）"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 降维主函数, PCA训练一个模型将向量投影到前k个主成分的较低维空间；返回df(spark格式)
def pcaCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnNames = requestDict['columnNames']
    # 新列列名，默认为“降维结果”，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = "降维结果"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 默认目标维度k为3，若用户指定，以指定为准
    try:
        k = int(requestDict['k'])
    except:
        k = 3

    # 目标维度k需要小于原始维度，否则返回错误信息
    if k >= len(columnNames):
        return "error_targetDimensions"

    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    vecAssembler = VectorAssembler(inputCols=columnNames, outputCol="features")
    try:
        df = vecAssembler.transform(df)
    except utils.IllegalArgumentException:
        return "error_numerical"

    # 设定pca模型
    pca = PCA(k=k, inputCol="features", outputCol=newColumnName)

    # 训练
    df = pca.fit(df).transform(df)

    df = df.drop("features")
    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '12'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 字符串转标签页面路由
@app.route('/stringIndexer', methods=['GET', 'POST'])
def stringIndexer():
    # 接受请求传参，例如: {"projectName":"订单分析","columnName":"客户名称","newColumnName":"客户名称(标签化，按频率排序，0为频次最高)"}
    # 参数中可指定分箱数numBuckets, 默认为5
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = stringIndexerCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行标签化"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 字符串转标签主函数, 将字符串转换成标签，加入新列；返回df(spark格式)
# 按label出现的频次，转换成0～分类个数-1，频次最高的转换为0，以此类推；如果输入的列类型为数值型，则会先转换成字符串型，再进行标签化
def stringIndexerCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 新列名称，默认为columnName + “(标签化，按频率排序，0为频次最高)”，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = columnName + "(标签化，按频率排序，0为频次最高)"
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 设定si（字符串转标签模型）
    si = StringIndexer(inputCol=columnName, outputCol=newColumnName)

    # 训练
    df = si.fit(df).transform(df)

    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '16'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 独热编码页面路由
@app.route('/oneHotEncoder', methods=['GET', 'POST'])
def oneHotEncoder():
    # 接受请求传参，例如: {"projectName":"订单分析","columnName":"数量","newColumnName":"数量(独热编码)"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = oneHotEncoderCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "error: 只能选择一列进行独热编码"
    elif df == "error_intOnly":
        return "error: 只能对数值型的列进行独热编码，且必须为整数或者形如[2.0]的索引，请检查列名输入是否有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 独热编码主函数, 将一个数值型的列转化成它的二进制形式；返回df(spark格式)
def oneHotEncoderCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnName = requestDict['columnName']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # 新列的列名默认为columnName + "(独热编码)"，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = columnName + "(独热编码)"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 设定独热编码模型
    ohe = OneHotEncoderEstimator(inputCols=[columnName], outputCols=[newColumnName])

    # 训练
    try:
        df = ohe.fit(df).transform(df)
    except:
        return "error_intOnly"

    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '7'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 多项式扩展页面路由
@app.route('/polynomialExpansion', methods=['GET', 'POST'])
def polynomialExpansion():
    # 接受请求传参，例如: {"projectName":"订单分析","columnNames":"数量,折扣,装运成本","newColumnName":"多项式扩展"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = polynomialExpansionCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_numerical":
        return "error: 多项式扩展只能应用于数值型的列，请检查列名输入是否有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 多项式扩展主函数, 将n维的原始特征组合扩展到多项式空间；返回df(spark格式)
# 功能说明：以degree为2的情况为例，输入为(x,y), 则输出为(x, x * x, y, x * y, y * y)
# 为了更加直观，举个带数字的例子，[0.5, 2.0] -> [0.5, 0.25, 2.0, 1.0, 4.0]
def polynomialExpansionCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnNamesStr = requestDict['columnNames']
    columnNames = columnNamesStr.split(",")
    # 新列的列名默认为"多项式扩展" + columnNames，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = "多项式扩展" + "(" + columnNamesStr + ")"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    vecAssembler = VectorAssembler(inputCols=columnNames, outputCol="features")
    try:
        df = vecAssembler.transform(df)
    except utils.IllegalArgumentException:
        return "error_numerical"

    # 设定多项式扩展模型
    px = PolynomialExpansion(inputCol="features", outputCol=newColumnName)

    # 训练
    df = px.transform(df)

    df = df.drop("features")
    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '9'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df


# 卡方选择页面路由
@app.route('/chiSqSelector', methods=['GET', 'POST'])
def chiSqSelector():
    # 接受请求传参，例如: {"projectName":"订单分析","columnNames":"折扣,装运成本","columnName_label":"数量","newColumnName":"多项式扩展","numTopFeatures":2}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 执行主函数，获取df(spark格式)
    df = chiSqSelectorCore(requestStr)
    if df == "error_projectUrl":
        return "error: 项目名或项目路径有误"
    elif df == "error_columnInputNumSingle":
        return "卡方基准列label只能选择一列，请检查列名输入是否有误"
    elif df == "error_columnInputNumMultiple":
        return "error: 用于卡方选择的列必须大于1，请检查列名输入是否有误"
    elif df == "error_numerical":
        return "error: 选择的列（包括用于卡方选择的列和卡方基准列label）都必须为数值型，请检查列名输入是否有误"

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    return jsonify({'length': df.count(), 'data': df_pandas.to_json(force_ascii=False)})


# 卡方选择主函数, 在特征向量中选择出那些“优秀”的特征，组成新的、更“精简”的特征向量；返回df(spark格式)
# 对特征和真实标签label之间进行卡方检验，来判断该特征和真实标签的关联程度，进而确定是否对其进行选择
# 对于label的选择，理论上来说要选择机器学习的目标列（若该列不是数值型，需将其数值化），但是无相应数据，本例中选择label="数量"，无实际意义
def chiSqSelectorCore(requestStr):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    columnNamesStr = requestDict['columnNames']
    columnName_label = requestDict['columnName_label']
    # columnName_label必须为单列，否则报错
    if len(columnName_label.split(",")) != 1:
        return "error_columnInputNumSingle"

    # 获取卡方选择结果topN的数目，默认numTopFeatures为1
    try:
        numTopFeatures = requestDict['numTopFeatures']
    except:
        numTopFeatures = 1

    columnNames = columnNamesStr.split(",")
    # columnNames的数目必须大于1，否则报错
    if len(columnNames) < 2:
        return "error_columnInputNumMultiple"
    # 新列的列名默认为"卡方选择" + (与 columnName_label 相关的前 numTopFeatures 个特征列)，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = "卡方选择" + "(与 [" + str(columnName_label) + "] 相关的前 " + str(numTopFeatures) + " 个特征列)"

    # spark会话
    spark = SparkSession \
        .builder \
        .master("local") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()

    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return 'error_projectUrl'  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']
    df = spark.read.csv(fileUrl, header=True, inferSchema=True)

    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    vecAssembler = VectorAssembler(inputCols=columnNames, outputCol="features")
    try:
        df = vecAssembler.transform(df)
    except utils.IllegalArgumentException:
        return "error_numerical"

    # 设定标签列label
    df = df.withColumn("label", df[columnName_label])

    # 设定多项式扩展模型
    selector = ChiSqSelector(numTopFeatures=numTopFeatures, outputCol=newColumnName)

    # 训练，若label的类型不是数值型，报错
    try:
        df = selector.fit(df).transform(df)
    except utils.IllegalArgumentException:
        return "error_numerical"

    df = df.drop("features")
    df.show()

    # 追加处理流程记录
    operateParameter = {}
    operateParameter['type'] = '13'
    operateParameter['operate'] = requestStr
    addProcessingFlow(projectName, "admin", operateParameter)

    return df