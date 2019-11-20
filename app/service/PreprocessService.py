# -*- coding: UTF-8 -*-
from app.Utils import *
import random
import string
from app.ConstFile import const
import app.dao.OperatorDao as OperatorDao
from pyspark.sql import functions as func
from pyspark.sql.functions import split, explode, concat_ws, regexp_replace

save_dir = const.SAVEDIR


def read_data_with_update_record(spark_session, operator_id, file_url):
    """
    读数据算子，拷贝数据并更新算子记录表

    :param spark_session:
    :param operator_id:
    :param file_url:
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 存储结果
        result_file_url = save_data(df)

        run_info = 'read_data算子执行成功'
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]
    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
        return []


def filter_multi_conditions(spark_session, operator_id, file_url, condition):
    """
    按照多个条件进行过滤

    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition: {"userId":1,"projectId":32,"parameter":[{"colName":"利润", "operate":">","value":"100", "relation":"AND"},{"colName":"装运方式", "operate":"==", "value":"一级", "relation":""}]}
    :return:
    """

    try:

        # condition_dict = json.loads(condition)
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 过滤函数
        result_df = filter_core(spark_session, df, condition['parameter'])
        # 存储结果
        result_file_url = save_data(result_df)

        run_info = '过滤算子执行成功'
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]
    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', result_file_url, run_info)
        traceback.print_exc()
        return []


def filter_core(spark, df, condition):
    """
    过滤的核心函数
    :param spark:
    :param df:
    :param condition:
    :return:
    """
    table_name = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    sql_str = 'select * from ' + table_name + ' where'
    # types = {}
    # for i in df.dtypes:
    #     types[i[0]] = i[1]
    #     print(i)
    for i in condition:
        if is_number(i['value']):
            sql_str = sql_str + ' `' + i['colName'] + '` ' + i['operate'] + ' ' + i['value'] + ' ' + i['relation']
        else:
            sql_str = sql_str + ' `' + i['colName'] + '` ' + i['operate'] + ' \"' + i['value'] + '\" ' + i['relation']
    print(sql_str)
    df.createOrReplaceTempView(table_name)
    sql_df = spark.sql(sql_str)

    return sql_df


def sort(spark_session, operator_id, file_url, condition):
    """
    排序

    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition: {"userId":1,"projectId":32,"columnName":"利润","sortType":"降序"}
    :return:
    """

    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 过滤函数
        result_df = sort_core(df, condition['columnName'], condition['sortType'])
        # 存储结果
        result_file_url = save_data(result_df)
        # TODO ：判断返回结果是否是String（异常信息）
        run_info = '排序算子执行成功'
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
        return []


def sort_core(df, column_name, column_type):
    """
    排序主函数，函数功能包括解析参数、排序；返回df(spark格式)
    :param df:
    :param column_name:
    :param column_type:
    :return:
    """
    # 只能输入一列，否则报错
    if len(column_name.split(",")) != 1:
        return "ERROR_NOT_ONLY_ONE_COL"

    # sortType默认为升序，若用户指定，以用户指定为准
    if (column_type == "") or (column_type is None):
        column_type = "升序"

    # 排序
    if column_type == "降序":
        df = df.sort(column_name, ascending=False)
    else:
        df = df.sort(column_name)
    return df


def column_split(spark_session, operator_id, file_url, condition):
    """
    按列拆分
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition:  {"userId": 1, "projectId": 32, "columnName": "订购日期", "delimiter": "/", "newColumnNames": ["year", "月"]}
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 拆分函数
        result_df = column_split_core(spark_session, df, condition)
        # 存储结果
        result_file_url = save_data(result_df)
        # 修改计算状态
        run_info = '拆分算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
        return []


def column_split_core(ss, df, condition_dict):
    """
    按列拆分主函数，返回dataFrame(spark格式)
    :param ss:
    :param df:
    :param condition_dict:
    :return:
    """
    # 参数解析
    column_name = condition_dict['columnName']
    delimiter = condition_dict['delimiter']
    new_column_names = condition_dict['newColumnNames']
    # 获取拆分出的新列的列名，若未指定，暂时存储为空列表，后续根据拆分数填充成为[x_split_1, x_split_2, ...]
    if new_column_names is None:
        new_column_names = []
    # 将指定列columnName按splitSymbol拆分，存入"splitColumn"列，列内数据格式为[a, b, c, ...]
    first_row = df.first()
    df_split = df.withColumn("splitColumn", split(df[column_name], delimiter))
    split_number = len(first_row[column_name].split(delimiter))
    # 若用户为指定拆分出的新列的列名，根据拆分数填充
    if len(new_column_names) == 0:
        for i in range(split_number):
            new_column_names.append(column_name + '_split_' + str(i + 1))
    # 给新列名生成索引，格式为：[('年', 0), ('月', 1), ('日', 2)]，方便后续操作
    sc = ss.sparkContext
    newColumnNames_with_index = sc.parallelize(new_column_names).zipWithIndex().collect()
    # 遍历生成新列
    for name, index in newColumnNames_with_index:
        df_split = df_split.withColumn(name, df_split["splitColumn"].getItem(index))
    df = df_split.drop("splitColumn")
    return df


def columns_merge(spark_session, operator_id, file_url, condition):
    """
    多列合并
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition: {"userId": 1, "projectId": 32, "columnNames": ["类别", "子类别", "产品名称"], "connector": "-", "newColumnName": "品类名称"}
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 合并函数
        result_df = columns_merge_core(df, condition)
        # 存储结果
        result_file_url = save_data(result_df)
        # 修改计算状态
        run_info = '多列合并算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
        return []


def columns_merge_core(df, condition_dict):
    """
    多列合并主函数，新增一列，列内的值为指定多列合并而成；返回df(spark格式)
    :param df:
    :param condition_dict:
    :return:
    """
    # 解析参数
    column_names = condition_dict['columnNames']
    split_symbol = condition_dict['connector']
    new_column_name = condition_dict['newColumnName']
    # 默认分隔符是","，或以用户指定为准
    if split_symbol is None or split_symbol == '':
        split_symbol = ','
    if new_column_name is None or new_column_name == '':
        new_column_name = '_'.join(new_column_name)
    # 合并
    column_list = []
    for i in range(len(column_names)):
        column_list.append(df[column_names[i]])
    df = df.withColumn(new_column_name, concat_ws(split_symbol, *column_list))
    return df


def replace(spark_session, operator_id, file_url, condition):
    """
    数据替换
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition: {"userId": 1, "projectId": 32, "columnNames": ["类别", "子类别", "客户名称"],"replaceCharacters":[{"source":"技术","target":"技术copy"},{"source":"电话","target":"电话copy"}]}
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 替换函数
        result_df = replace_core(df, condition)
        # 存储结果
        result_file_url = save_data(result_df)
        # 修改计算状态
        run_info = '数据替换算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
        return []


def replace_core(df, condition_dict):
    """
    数据列替换主函数, 将多个列中的字符进行替换；返回df(spark格式)
    :param df:
    :param condition_dict:
    :return:
    """

    def mul_regexp_replace(col):
        """
        对每一列进行替换
        :param col:
        :return:
        """
        for item in replace_characters:
            col = regexp_replace(col, item["source"], item["target"])
        return col

    # 解析参数
    column_names = condition_dict['columnNames']
    replace_characters = condition_dict['replaceCharacters']
    # 对每一列进行替换
    for i in range(len(column_names)):
        column_name = column_names[i]
        df = df.withColumn(column_name, (mul_regexp_replace(df[column_name])))
    return df


def fill_null_value(spark_session, operator_id, file_url, condition):
    """
    填充空值
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition: {'userId':1,'projectId':32,'parameter':[{'operate':'均值填充','colName':''},{'operate':'均值填充','colName':'最大值填充'}]}
    :return:
    """

    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 空值填充函数
        result_df = fill_null_value_core(df, condition["parameter"])
        # 存储结果
        result_file_url = save_data(result_df)
        # 修改计算状态
        run_info = '数据替换算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
        return []


def fill_null_value_core(df, condition):
    """
    填充空值核心函数
    :param df:
    :param condition:
    :return:
    """
    for i in condition:
        if i['operate'] == '均值填充':
            mean_item = df.select(func.mean(i['colName'])).collect()[0][0]
            df = df.na.fill({i['colName']: mean_item})
        elif i['operate'] == '最大值填充':
            mean_item = df.select(func.max(i['colName'])).collect()[0][0]
            df = df.na.fill({i['colName']: mean_item})
        elif i['operate'] == '最小值填充':
            mean_item = df.select(func.min(i['colName'])).collect()[0][0]
            df = df.na.fill({i['colName']: mean_item})
    return df


def column_map(spark_session, operator_id, file_url, condition):
    """
    列映射
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition:{"userId":1,"projectId":32,"parameter":[{"colName_1":"利润", "operate_1":"+","value_1":"100","operate":"+","colName_2":"数量", "operate_2":"*","value_2":"0.0001","newName":"newCol1"},{"colName_1":"利润", "operate_1":"+","value_1":"10","operate":"*","colName_2":"数量", "operate_2":"*","value_2":"0.1","newName":"newCol2"}]}
    :return:
    """

    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 列映射函数
        result_df = column_map_core(df, condition["parameter"])
        # 存储结果
        result_file_url = save_data(result_df)
        # 修改计算状态
        run_info = '列映射算子执行成功'
        OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
        return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
        return []


def column_map_core(df, condition):
    """
    列映射核心函数
    :param df:
    :param condition:
    :return:
    """
    for i in condition:
        name1 = i['colName_1']
        name2 = i['colName_2']
        new_name = i['newName']
        if i['operate_1'] == '+':
            df = df.withColumn(new_name, df[name1] + i['value_1'])
        elif i['operate_1'] == '-':
            df = df.withColumn(new_name, df[name1] - i['value_1'])
        elif i['operate_1'] == '*':
            df = df.withColumn(new_name, df[name1] * i['value_1'])
        elif i['operate_1'] == '/':
            df = df.withColumn(new_name, df[name1] / i['value1_'])
        if not ((name2 == "") or (name2 is None)):
            new_name2 = new_name + "_2"
            if i['operate_2'] == '+':
                df = df.withColumn(new_name2, df[name2] + i['value_2'])
            elif i['operate_2'] == '-':
                df = df.withColumn(new_name2, df[name2] - i['value_2'])
            elif i['operate_2'] == '*':
                df = df.withColumn(new_name2, df[name2] * i['value_2'])
            elif i['operate_2'] == '/':
                df = df.withColumn(new_name2, df[name2] / i['value_2'])

            if i['operate'] == '+':
                df = df.withColumn(new_name, df[new_name] + df[new_name2])
            elif i['operate'] == '-':
                df = df.withColumn(new_name, df[new_name] - df[new_name2])
            elif i['operate'] == '*':
                df = df.withColumn(new_name, df[new_name] * df[new_name2])
            elif i['operate'] == '/':
                df = df.withColumn(new_name, df[new_name] / df[new_name2])
            df = df.drop(new_name2)
    return df
