# -*- coding: UTF-8 -*-
from app.models.MSEntity import Project, ProcessFlow
import os, json, time
from app import db
import pandas as pd
from pyspark.sql import SparkSession
import uuid, shutil, traceback
from flask.json import jsonify
from app.ConstFile import const


def list_str_to_list(str):
    """
    字符串数组
    :param str:  "["1","2"]"
    :return:
    """
    if str is None or str == '':
        return {}
    change = json.loads("{\"key\":" + str.replace("'", "\"") + "}")
    return change['key']


# 返回数据
def returnDataModel(df, state, reason):
    if state:
        return jsonify({'state': state, 'reason': reason, 'length': df.count(), 'data': dfToJson(df, 50)})
    else:
        return jsonify({'state': state, 'reason': reason, 'length': 0, 'data': {}})


# 获取时间戳
def funTime():
    t = time.time()
    return str(int(round(t * 1000)))  # 毫秒级时间戳


# 获取一个新的SparkSession
def getSparkSession(userId, computationName):
    appName = str(userId) + "_" + computationName + '_' + str(funTime())
    print('Spark Session Name: ', appName)
    # ss = SparkSession \
    #     .builder \
    #     .appName(appName) \
    #     .master("spark://10.108.211.130:7077") \
    #     .getOrCreate()

    ss = SparkSession \
        .builder \
        .appName(appName) \
        .master("local[*]") \
        .getOrCreate()
    return ss


# 返回前nums条数据（json格式）
def dfToJson(df, nums):
    data_1 = df.limit(nums).toJSON().collect()
    data_2 = ",".join(data_1)
    data_3 = '[' + data_2 + ']'
    return json.loads(data_3)


# 获取处理流
def getProcessFlowByProjectId(projectId):
    try:
        filters = {
            ProcessFlow.project_id == projectId,
        }
        return ProcessFlow.query.filter(*filters).first()
    except:
        return "error"


# 追加处理流程记录
def addProcessingFlow(projectName, userId, operateType, operateParameter):
    try:
        operate = {}
        operate['type'] = operateType
        operate['key'] = str(uuid.uuid1())
        print(operate['key'])
        operate['operate'] = operateParameter
        print("追加处理流程", projectName, userId, operate)
        pflow = db.session.query(ProcessFlow.id, ProcessFlow.project_id, ProcessFlow.operates, ProcessFlow.cur_ope_id,
                                 ProcessFlow.links). \
            join(Project, Project.id == ProcessFlow.project_id). \
            filter(Project.project_name == projectName). \
            filter(Project.user_id == userId). \
            first()
        # 修改 operates
        # print(len(pflow))
        if not (pflow[2] == None or pflow[2] == ""):
            operates = json.loads(pflow[2])
        else:
            operates = []
        operates.append(operate)
        operateStr = json.dumps(operates, ensure_ascii=False)
        # 修改 links
        if not (pflow[3] == None or pflow[3] == ""):
            link = {}
            link['from'] = pflow[3]
            link['to'] = operate['key']
            if not (pflow[4] == None or pflow[4] == ""):
                links = json.loads(pflow[4])
            else:
                links = []
            links.append(link)
            linkStr = json.dumps(links, ensure_ascii=False)
        else:
            linkStr = pflow[4]
        filters = {
            ProcessFlow.id == pflow[0],
        }
        result = ProcessFlow.query.filter(*filters).first()
        result.operates = operateStr
        result.links = linkStr
        result.cur_ope_id = operate['key']
        db.session.commit()
        return ""
    except Exception:
        print('traceback.format_exc():\n%s' % traceback.format_exc())
        print("追加数据流程出错")
        return "追加数据流程出错"


# addProcessingFlow('甜点销售数据预处理',1,{'type':'1','operate':'列名一,关系,值,组合关系;列名一,关系,值,'})

# 获取项目
def getProjectByNameAndUserId(projectName, userId):
    try:
        print('projectName=', projectName, ' userId=', userId)
        return db.session.query(Project).filter(Project.project_name == projectName) \
            .filter(Project.user_id == userId) \
            .first()
    except:
        return "error"


# 获取项目的正在操作的数据文件地址
def getProjectCurrentDataUrl(projectName):
    try:
        filters = {
            Project.project_name == projectName
        }
        pro = Project.query.filter(*filters).first()
        project_address = pro.project_address
        filename = ''
        for root, dirs, files in os.walk(project_address):
            # print(root) #当前目录路径
            # print(dirs) #当前路径下所有子目录
            # print(files) #当前路径下所有非目录子文件
            for file in files:
                if file[-4:] == '.csv':
                    filename = file
                    break
            break
        # print(filename)
        if filename == '':
            return "error"
        else:
            # return {'fileUrl': ProjectAddress+'/'+filename, 'projectAddress': ProjectAddress}
            return {'fileUrl': 'file://' + project_address + '/' + filename, 'projectAddress': project_address}
    except:
        return "error"


# 获取项目的正在操作的文件数据
def getProjectCurrentData(ss, projectName):
    # 解析项目路径，读取csv
    urls = getProjectCurrentDataUrl(projectName)
    if urls == 'error':
        return "error: 项目名或项目路径有误"  # 错误类型：项目名或项目路径有误
    fileUrl = urls['fileUrl']  # 读本地文件
    df = ss.read.csv(fileUrl, header=True, inferSchema=True)
    # ss.
    # import pandas as pd
    # sc = ss.sparkContext
    # sqlContext = SQLContext(sc)
    # df = pd.read_csv(fileUrl)
    # df = sqlContext.createDataFrame(df)

    # df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', inferschema='true').load(fileUrl)
    return df


def read_data_pandas(file_url):
    """
    pandas 读取数据
    :param file_url:
    :return:
    """
    if file_url[-4:] == ".csv":
        df = pd.read_csv(file_url, encoding="utf-8")
    else:
        df = pd.read_excel(file_url, encoding="utf-8")
    return df


def save_data_pandas(data, file_type="", file_url="", index=0):
    """
    pandas 写数据
    :return:
    """
    if file_type == "":
        file_type = 'csv'
    if file_url == "":
        file_url = const.MIDDATA + str(uuid.uuid1())

    if file_type == 'json':
        file_url = file_url + '.json'
        json_str = json.dumps(data, ensure_ascii=False)
        with open(file_url, "w", encoding="utf-8") as f:
            json.dump(json_str, f, ensure_ascii=False)
    elif file_type == 'csv':
        file_url = file_url + '.csv'
        data.to_csv(file_url, header=True, index=index)

    return file_url


def read_data(ss, file_url):
    """
    spark 读取数据
    :param ss:spark session
    :param file_url:
    :return:
    """

    df = ss.read.csv(file_url, header=True, inferSchema=True)
    return df


def save_data(df, file_url=""):
    """
    保存数据
    :param df:
    :param file_url:
    :return:
    """
    if file_url == "":
        file_url = const.MIDDATA + str(uuid.uuid1()) + '.csv'
    df.toPandas().to_csv(file_url, header=True, index=0)
    return file_url


def mkdir(path):
    """
    根据指定路径创建文件夹
    :param path:
    :return:
    """
    import os

    # 去除首位空格
    path = path.strip()
    # 去除尾部 \ 符号
    path = path.rstrip("\\")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists = os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        print(path + ' 创建成功')
        # 创建目录操作函数
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path + ' 目录已存在')
        return False


def deldir(path):
    import os
    if os.path.exists(path):
        # 删除文件，可使用以下两种方法。
        os.remove(path)
        return True
    else:
        print('no such file:%s' % path)
        return False


def deltree(path):
    import os
    if os.path.exists(path):
        shutil.rmtree(path)  # 递归删除文件夹
        return True
    else:
        print('no such path:%s' % path)
        return False


def is_number(s):
    try:
        float(s)
        return True
    except ValueError:
        pass

    try:
        import unicodedata
        unicodedata.numeric(s)
        return True
    except (TypeError, ValueError):
        pass
    return False
