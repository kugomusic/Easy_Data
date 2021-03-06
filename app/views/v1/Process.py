# -*- coding: UTF-8 -*-
from flask import request, jsonify, Response
from app import app
from app.Utils import *
from app.enmus.EnumConst import *
from pyspark.sql.functions import split, explode, concat_ws, regexp_replace
import random, string
from app.ConstFile import const
from datetime import datetime

"""
v1版本，弃用
"""

save_dir = const.SAVEDIR


# 欢迎页面
@app.route("/", methods=['GET', 'POST'])
def hello():
    return "<h1 style='color:blueviolet'> HomePage of Easy_Data</h1>"


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


@app.route("/filter", methods=['GET', 'POST'])
def filterMultiConditions():
    """
    接受请求传参，例如: {"userId":"1","projectName":"特征工程测试项目","parameter":[{"colName":"利润", "operate":">", "value":"100", "relation":"AND"},{"colName":"装运方式", "operate":"==", "value":"一级", "relation":""}]}

    :return:
    """
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")
    a = datetime.now()
    ## 参数测试例子
    # requestDict = {"userId": "1", "projectName": "特征工程测试项目",
    #                "parameter": [{"colName": "利润", "operate": ">", "value": "100", "relation": "AND"},
    #                              {"colName": "装运方式", "operate": "==", "value": "一级", "relation": ""}]}
    # requestStr = json.dumps(requestDict)
    requestDict = json.loads(requestStr)
    projectName = requestDict['projectName']  # 项目名称
    userId = requestDict['userId']  # 用户ID
    parameter = requestDict['parameter']
    functionName = "filter"
    state = True
    reason = ""
    print(functionName, projectName, userId, requestDict)
    # spark会话
    ss = getSparkSession(userId, functionName)
    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        state = False
        reason = df
        return returnDataModel(df, state, reason)
    # 过滤函数
    sqlDF = filterCore(ss, df, parameter)
    # 处理后的数据写入文件
    # sqlDF.write.csv(path='/home/zk/data/test.csv', header=True, sep=",", mode="overwrite")
    # sqlDF.coalesce(1).write.option("header", "true").csv("/home/zk/data/test.csv")
    sqlDF.toPandas().to_csv(save_dir, header=True)
    # 追加处理流程记录
    resultStr = addProcessingFlow(projectName, userId, OperatorType.FILTER.value, requestStr)
    if resultStr != "":
        state = False
        reason = reason + "  " + resultStr
    # 返回前50条数据
    b = datetime.now()
    print('-------------------------', b - a)
    return returnDataModel(df, state, reason)


def filterCoreParameter(projectName, parameterStr):
    try:
        urls = getProjectCurrentDataUrl(projectName)
        fileUrl = urls['fileUrl']
    except:
        return "error"
    parameter = {}
    parameter['fileUrl'] = fileUrl
    condition = []
    strList = parameterStr[0:len(parameterStr) - 1].split(';')
    for i in range(len(strList) - 1):
        if strList[i] == "" or strList[i] == None:
            continue
        ll = strList[i].split(',', 3)
        con = {}
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
    sqlStr = 'select * from ' + tableName + ' where'
    # types = {}
    # for i in df.dtypes:
    #     types[i[0]] = i[1]
    #     print(i)
    for i in condition:
        if is_number(i['value']):
            sqlStr = sqlStr + ' `' + i['colName'] + '` ' + i['operate'] + ' ' + i['value'] + ' ' + i['relation']
        else:
            sqlStr = sqlStr + ' `' + i['colName'] + '` ' + i['operate'] + ' \"' + i['value'] + '\" ' + i['relation']
    print(sqlStr)
    df.createOrReplaceTempView(tableName)
    sqlDF = spark.sql(sqlStr)

    return sqlDF


# 排序 页面路由
@app.route("/sort", methods=['GET', 'POST'])
def sort():
    # 接受请求传参，例如: {"userId":"1","projectName":"订单分析","columnName":"利润","sortType":"降序"}
    ## 接收参数
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)

    a = datetime.now()

    ## 参数示例
    # requestDict = {"userId": "1", "projectName": "特征工程测试项目", "columnName": "利润", "sortType": "升序"}
    # requestStr = json.dumps(requestDict)

    projectName = requestDict['projectName']  # 项目名称
    userId = requestDict['userId']  # 用户ID
    functionName = "sort"
    state = True
    reason = ""
    print(functionName, projectName, userId, requestStr)
    # spark会话
    ss = getSparkSession(userId, functionName)

    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        state = False
        reason = df
        return returnDataModel(df, state, reason)

    # 执行主函数，获取df(spark格式)
    df = sortCore(requestDict, df)
    if df == "ERROR_NOT_ONLY_ONE_COL":
        state = False
        reason = "error: 只能选择一列进行排序"
        return returnDataModel(df, state, reason)

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df.toPandas().to_csv(save_dir, header=True)

    # 追加处理流程记录
    resultStr = addProcessingFlow(projectName, userId, OperatorType.SORT.value, requestStr)
    if resultStr != "":
        state = False
        reason = resultStr
    # 返回前50条数据
    b = datetime.now()
    print('-------------------------', b - a)
    return returnDataModel(df, state, reason)


# 排序主函数，函数功能包括解析参数、排序；返回df(spark格式)
def sortCore(requestDict, df):
    columnName = requestDict['columnName']  # 排序的列名（只能是一列）
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "ERROR_NOT_ONLY_ONE_COL"
    # sortType默认为升序，若用户指定，以用户指定为准
    try:
        sortType = requestDict['sortType']  # 排序类型（升序、降序）
        if sortType == "":
            sortType = "升序"
    except:
        sortType = "升序"

    # 排序
    if sortType == "降序":
        df = df.sort(columnName, ascending=False)
    else:
        df = df.sort(columnName)
    return df


# 排序 请求参数及返回值
@app.route("/jsontostrTest", methods=['GET', 'POST'])
def jsontostrTest():
    req = {}
    parameter = {}
    parameter["userId"] = "1"
    parameter["projectName"] = "特征工程测试项目"
    parameter["columnName"] = "利润"
    parameter["sortType"] = "升序"

    req["methods"] = "POST"
    req["url"] = "http://10.108.211.130:8993/sort"
    req["parameter"] = json.dumps(parameter, ensure_ascii=False)
    req["result"] = {"data": [], "length": 35}
    # print(json.dumps(parameter))
    return json.dumps(parameter, ensure_ascii=False)


# 按列拆分页面路由
@app.route("/columnSplit", methods=['GET', 'POST'])
def columnSplit():
    # # 接受请求传参，例如: {"userId":"1","project":"订单分析","columnName":"订购日期","newColumnNames":["年","月","日"]}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)

    a = datetime.now()

    ## 参数示例
    # requestDict = {"userId": "1", "projectName": "特征工程测试项目", "columnName": "订购日期", "delimiter": "/",
    #                "newColumnNames": ["year", "月"]}
    # requestStr = json.dumps(requestDict)

    userId = requestDict['userId']  # 用户ID
    projectName = requestDict['projectName']
    functionName = "columnSplit"
    state = True
    reason = ""
    print(functionName, projectName, userId, requestStr)
    # spark会话
    ss = getSparkSession(userId, functionName)

    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        state = False
        reason = df
        return returnDataModel(df, state, reason)

    # 执行主函数，获取df(spark格式)
    df = columnSplitCore(ss, df, requestDict)
    if df == "ERROR_NOT_ONLY_ONE_COL":
        state = False
        reason = "error: 只能选择一列进行排序"
        return returnDataModel(df, state, reason)

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    # 追加处理流程记录
    resultStr = addProcessingFlow(projectName, userId, OperatorType.COLUMNSPLIT.value, requestStr)
    if resultStr != "":
        state = False
        reason = resultStr
    # 返回前50条数据
    b = datetime.now()
    print('-------------------------', b - a)
    return returnDataModel(df, state, reason)


# 按列拆分主函数，函数功能包括解析参数、拆分；返回df(spark格式)
def columnSplitCore(ss, df, requestDict):
    # 对参数格式进行转化：json->字典，并进一步进行解析
    columnName = requestDict['columnName']
    delimiter = requestDict['delimiter']
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "ERROR_NOT_ONLY_ONE_COL"
    # 获取拆分出的新列的列名，若未指定，暂时存储为空列表，后续根据拆分数填充成为[拆分列1,拆分列2,拆分列3,...]
    try:
        newColumnNames = requestDict['newColumnNames']
    except:
        newColumnNames = []
    # 将指定列columnName按splitSymbol拆分，存入"splitColumn"列，列内数据格式为[a, b, c, ...]
    print('---------------------------------------------------')
    # df.show()
    # df.head()
    first_row = df.first()
    df_split = df.withColumn("splitColumn", split(df[columnName], delimiter))
    splitNumber = len(first_row[columnName].split(delimiter))
    # 若用户为指定拆分出的新列的列名，根据拆分数填充
    if newColumnNames == []:
        for i in range(splitNumber):
            newColumnNames.append("拆分列" + str(i + 1))
    # 给新列名生成索引，格式为：[('年', 0), ('月', 1), ('日', 2)]，方便后续操作
    sc = ss.sparkContext
    newColumnNames_with_index = sc.parallelize(newColumnNames).zipWithIndex().collect()
    # 遍历生成新列
    for name, index in newColumnNames_with_index:
        df_split = df_split.withColumn(name, df_split["splitColumn"].getItem(index))
    df = df_split.drop("splitColumn")
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
    userId = '1'
    functionName = "rowSplit"
    print(functionName, projectName, userId, requestDict)
    # 只能输入一列，否则报错
    if len(columnName.split(",")) != 1:
        return "error_columnInputNumSingle"
    # newColumnName默认为columnName+“拆分”，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = columnName + "拆分"

    # spark会话
    ss = getSparkSession(userId, functionName)

    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        return df

    # 拆分
    first_row = df.first()
    splitStr = first_row[columnName]
    # 识别splitStr中的符号
    splitSymbol = symbolRecognition(splitStr)
    if splitSymbol == '':
        return "error_splitSymbol"  # 错误类型：指定列中不含可供拆分的符号

    # 将指定列columnName按splitSymbol拆分，存入newColumnName列的多行
    df = df.withColumn(newColumnName, explode(split(df[columnName], splitSymbol)))
    df.show()

    # 追加处理流程记录
    addProcessingFlow(projectName, userId, OperatorType.ROWSPLIT.value, requestStr)
    return df


# 多列合并页面路由
@app.route("/columnsMerge", methods=['GET', 'POST'])
def columnsMerge():
    # 接受请求传参，例如: {"projectName":"订单分析","columnNames":["类别","子类别","产品名称"],"newColumnName":"品类名称","splitSymbol":"-"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")
    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)

    a = datetime.now()

    ## 参数示例
    # requestDict = {"userId": "1", "projectName": "订单分析", "columnNames": ["类别", "子类别", "产品名称"], "connector": "-",
    #                "newColumnName": "品类名称"}
    # requestStr = json.dumps(requestDict)

    projectName = requestDict['projectName']
    userId = '1'
    functionName = "columnsMerge"
    state = True
    reason = ""
    print(functionName, projectName, userId, requestDict)
    # spark会话
    ss = getSparkSession(userId, functionName)

    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        state = False
        reason = df
        return returnDataModel(df, state, reason)

    # 执行主函数，获取df(spark格式)
    df = columnsMergeCore(df, requestDict)

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    # 追加处理流程记录
    resultStr = addProcessingFlow(projectName, userId, OperatorType.COLUMNMERGE.value, requestStr)
    if resultStr != "":
        state = False
        reason = resultStr
    # 返回前50条数据
    b = datetime.now()
    print('-------------------------', b - a)
    return returnDataModel(df, state, reason)


# 多列合并主函数，新增一列，列内的值为指定多列合并而成；返回df(spark格式)
def columnsMergeCore(df, requestDict):
    columnNames = requestDict['columnNames']
    # 默认分隔符是","，若requestStr中指定了分隔符，则以用户指定为准
    try:
        splitSymbol = requestDict['connector']
    except:
        splitSymbol = ','
    # 默认新列名称为：合并结果(col1, col2, col3, ...)，若用户指定，以用户指定为准
    try:
        newColumnName = requestDict['newColumnName']
    except:
        newColumnName = "合并结果" + "(" + str(columnNames).strip("[]") + ")"

    # 合并(spark的dataframe操作好蠢，暂时先用笨办法合并吧 >_< )
    if len(columnNames) == 2:
        df = df.withColumn(newColumnName, concat_ws(splitSymbol, df[columnNames[0]], df[columnNames[1]]))
    elif len(columnNames) == 3:
        df = df.withColumn(newColumnName,
                           concat_ws(splitSymbol, df[columnNames[0]], df[columnNames[1]], df[columnNames[2]]))
    elif len(columnNames) == 4:
        df = df.withColumn(newColumnName,
                           concat_ws(splitSymbol, df[columnNames[0]], df[columnNames[1]], df[columnNames[2]],
                                     df[columnNames[3]]))
    return df


# 数据列替换 页面路由
@app.route("/replace", methods=['GET', 'POST'])
def replace():
    # # 接受请求传参，例如: {"project":"订单分析","columnName":"客户ID","sourceCharacter":"0","targetCharacter":"Q"}
    if request.method == 'GET':
        requestStr = request.args.get("requestStr")
    else:
        requestStr = request.form.get("requestStr")

    # 对参数格式进行转化：json->字典，并进一步进行解析
    requestDict = json.loads(requestStr)

    a = datetime.now()
    ## 参数示例
    # requestDict = {"userId": "1", "projectName": "订单分析", "columnNames": ["类别", "子类别", "客户名称"],
    #                "sourceCharacters": ["技术", "电话", "CraigReiter", "复印机"],
    #                "targetCharacters": ["技术copy", "电话copy", "CraigReitercopy", "复印机copy"]}
    # requestStr = json.dumps(requestDict)

    projectName = requestDict['projectName']
    userId = '1'
    functionName = "replace"
    state = True
    reason = ""
    print(functionName, projectName, userId, requestDict)
    # spark会话
    ss = getSparkSession(userId, functionName)

    # 解析项目路径，读取csv
    df = getProjectCurrentData(ss, projectName)
    if df == "error: 项目名或项目路径有误":
        state = False
        reason = df
        return returnDataModel(df, state, reason)

    # 执行主函数，获取df(spark格式)
    df = replaceCore(df, requestDict)

    # 处理后的数据写入文件（借助pandas进行存储、返回）
    df_pandas = df.toPandas()
    df_pandas.to_csv(save_dir, header=True)

    # 追加处理流程记录
    resultStr = addProcessingFlow(projectName, userId, OperatorType.REPLACE.value, requestStr)
    if resultStr != "":
        state = False
        reason = resultStr
    # 返回前50条数据
    b = datetime.now()
    print('-------------------------', b - a)
    return returnDataModel(df, state, reason)


# 数据列替换主函数, 将某列中的字符进行替换；返回df(spark格式)
def replaceCore(df, requestDict):
    columnNames = requestDict['columnNames']
    sourceCharacters = requestDict['sourceCharacters']
    targetCharacters = requestDict['targetCharacters']

    if len(sourceCharacters) != len(targetCharacters):
        return "被替换的字符和用于替换的字符个数不相等"
    for i in range(len(columnNames)):
        # 字符替换
        columnName = columnNames[i]
        df = df.withColumn("temp", (mul_regexp_replace(df[columnName], sourceCharacters, targetCharacters)))
        df = df.drop(columnName)
        df = df.withColumnRenamed("temp", columnName)
    return df


def mul_regexp_replace(col, sourceCharacters, targetCharacters):
    for i in range(len(sourceCharacters)):
        col = regexp_replace(col, sourceCharacters[i], targetCharacters[i])
    return col


@app.route("/testTime", methods=['GET', 'POST'])
def testTime():
    a = datetime.now()
    filterMultiConditions()
    sort()
    columnSplit()
    columnsMerge()
    replace()
    b = datetime.now()
    print('-------------------------', b - a)
