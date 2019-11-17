# -*- coding: UTF-8 -*-
from app.Utils import *
import random
import string
from app.ConstFile import const
import app.dao.OperatorDao as OperatorDao

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

        run_info = 'filter算子执行成功'
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
        # 对参数格式进行转化：json->字典，并进一步进行解析
        # condition_dict = json.loads(condition)
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 过滤函数
        result_df = sort_core(df, condition['columnName'], condition['sortType'])
        # 存储结果
        result_file_url = save_data(result_df)
        # TODO ：判断返回结果是否是String（异常信息）
        run_info = 'sort算子执行成功'
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
