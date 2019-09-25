# -*- coding: UTF-8 -*-
from flask import request, jsonify, Response
from app import app
from app.utils import *
from app.enmus.EnumConst import operatorType
from app.constFile import const

save_dir = const.SAVEDIR


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


# 填充空值
@app.route("/fillNullValue", methods=['GET', 'POST'])
def fillNullValue():
    # 接受请求传参，例如: {"project":"订单分析","columnName":"客户ID","sourceCharacter":"0","targetCharacter":"Q"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']
    userId = requestDict['userId']
    parameter = requestDict['parameter']
    functionName = "fillNullValue"
    state = True
    reason = ""
    print(functionName, projectName, userId, requestDict)
    ss = getSparkSession(userId, functionName)
    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        state = False
        reason = df
        return returnDataModel(df, state, reason)
    # 过滤函数
    sqlDF = fillNullValueCore(ss, df, parameter)
    # 处理后的数据写入文件
    sqlDF.toPandas().to_csv(save_dir, header=True)
    # 追加处理流程记录
    resultStr = addProcessingFlow(projectName, userId, operatorType.FILLNULLVALUE.value, requestStr)
    if resultStr != "":
        state = False
        reason = resultStr
    # 返回前50条数据
    return returnDataModel(df, state, reason)


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
        con = {}
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
        if (i['operate'] == '均值填充'):
            mean_item = df.select(func.mean(i['colName'])).collect()[0][0]
            df = df.na.fill({i['colName']: mean_item})
        elif (i['operate'] == '最大值填充'):
            mean_item = df.select(func.max(i['colName'])).collect()[0][0]
            df = df.na.fill({i['colName']: mean_item})
        elif (i['operate'] == '最小值填充'):
            mean_item = df.select(func.min(i['colName'])).collect()[0][0]
            df = df.na.fill({i['colName']: mean_item})
    return df


@app.route("/columnMap", methods=['GET', 'POST'])
def columnMap():
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)
    userId = requestDict['userId']  # 用户ID
    projectName = requestDict['projectName']
    parameter = requestDict['parameter']
    functionName = "columnMap"
    state = True
    reason = ""
    print(functionName, projectName, userId, requestDict)
    # 获取sparkSession
    ss = getSparkSession(userId, functionName)
    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        state = False
        reason = df
        return returnDataModel(df, state, reason)
    # 过滤函数
    sqlDF = columnMapCore(ss, df, parameter)
    # 处理后的数据写入文件
    sqlDF.toPandas().to_csv(save_dir, header=True)
    # 追加处理流程记录
    resultStr = addProcessingFlow(projectName, userId, operatorType.COLUMNMAP.value, requestStr)
    if resultStr != "":
        state = False
        reason = resultStr
    # 返回前50条数据
    return returnDataModel(df, state, reason)


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
        con = {}
        con['colName_1'] = ll[0]
        con['operate_1'] = ll[1]
        con['value_1'] = ll[2]

        con['operate'] = ll[3]
        con['colName_2'] = ll[4]
        con['operate_2'] = ll[5]
        con['value_2'] = ll[6]

        con['newName'] = ll[7]
        condition.append(con)
    parameter['condition'] = condition
    return parameter


from pyspark.sql import functions as func


def columnMapCore(spark, df, condition):
    # val fillColValues = Map("StockCode" -> 5, "Description" -> "No value")
    # df.na.fill(fillColValues)
    for i in condition:
        name1 = i['colName_1']
        name2 = i['colName_2']
        newName = i['newName']
        if (i['operate_1'] == '+'):
            df = df.withColumn(newName, df[name1] + i['value_1'])
        elif (i['operate_1'] == '-'):
            df = df.withColumn(newName, df[name1] - i['value_1'])
        elif (i['operate_1'] == '*'):
            df = df.withColumn(newName, df[name1] * i['value_1'])
        elif (i['operate_1'] == '/'):
            df = df.withColumn(newName, df[name1] / i['value1_'])
        if (not ((name2 == "") or (name2 == None))):
            newName2 = newName + "_2"
            if (i['operate_2'] == '+'):
                df = df.withColumn(newName2, df[name2] + i['value_2'])
            elif (i['operate_2'] == '-'):
                df = df.withColumn(newName2, df[name2] - i['value_2'])
            elif (i['operate_2'] == '*'):
                df = df.withColumn(newName2, df[name2] * i['value_2'])
            elif (i['operate_2'] == '/'):
                df = df.withColumn(newName2, df[name2] / i['value_2'])

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
