# -*- coding: UTF-8 -*-
from app.Utils import *
import app.dao.OperatorDao as OperatorDao
import pandas as pd


def full_table_statistics(spark_session, operator_id, file_url, condition):
    """
    全表统计
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition:
    :return:
    """

    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data_pandas(file_url)
        # 全表统计函数
        result_df = full_table_statistics_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data_pandas(result_df, '', '', 1)
            run_info = '全表统计算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def full_table_statistics_core(df, condition):
    """
    全表统计核心函数
    :param df: 数据（pandas）
    :param condition: {"projectId": 32, "columnNames": ['利润']}
    :return:
    """
    column_names = condition['columnNames']
    statistics = ['类型', '总数', '最小值', '最小值位置', '25%分位数', '中位数', '75%分位数', '均值', '最大值', '最大值位置', '平均绝对偏差', '方差',
                  '标准差', '偏度', '峰度']
    data = {}
    for columnName in column_names:
        info = []
        column_type = df[columnName].dtype
        if column_type == 'int64' or column_type == 'float64':
            info.append('number')
            info.append(str(df[columnName].count()))
            info.append(str(df[columnName].min()))
            info.append(str(df[columnName].idxmin()))
            info.append(str(df[columnName].quantile(.25)))
            info.append(str(df[columnName].median()))
            info.append(str(df[columnName].quantile(.75)))
            info.append(str(df[columnName].mean()))
            info.append(str(df[columnName].max()))
            info.append(str(df[columnName].idxmax()))
            info.append(str(df[columnName].mad()))
            info.append(str(df[columnName].var()))
            info.append(str(df[columnName].std()))
            info.append(str(df[columnName].skew()))
            info.append(str(df[columnName].kurt()))
        else:
            info.append('text')
            info.append(str(df[columnName].count()))
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
            info.append('')
        data[columnName] = info
        print(pd.DataFrame(data, index=statistics))
    return pd.DataFrame(data, index=statistics)


def frequency_statistics(spark_session, operator_id, file_url, condition):
    """
    频次统计
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition:
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data_pandas(file_url)
        # 频次统计函数
        result_df = frequency_statistics_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data_pandas(result_df)
            run_info = '频次统计算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def frequency_statistics_core(df, condition):
    """
    :param df:
    :param condition:{"projectId":32,"columnName":"Item"}
    :return:
    """
    column_name = condition['columnName']

    s = df[column_name].value_counts()
    data = {column_name: s.index, "频率": s.values}
    print(pd.DataFrame(data))
    return pd.DataFrame(data)


def correlation_coefficient(spark_session, operator_id, file_url, condition):
    """
    相关系数
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition:
    :return:
    """
    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data_pandas(file_url)
        # 相关系数函数
        result_df = correlation_coefficient_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data_pandas(result_df, '', '', 1)
            run_info = '相关系数算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def correlation_coefficient_core(df, condition):
    """
    :param df:
    :param condition: {"projectId": 32, "columnNames": ["销售额", "折扣", "装运成本"]}
    :return:
    """
    column_names = condition['columnNames']
    # 报错信息：如果所选列不是数值型，则报错
    accept_types = ['int64', 'float64']
    for columnName in column_names:
        if df[columnName].dtype not in accept_types:
            return "只能计算数值型列的相关系数，但是 <" + columnName + "> 的类型为 " + str(df[columnName].dtype)
    # 计算出相关系数矩阵df
    return df.corr()
