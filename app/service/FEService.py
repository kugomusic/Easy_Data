# -*- coding: UTF-8 -*-
"""
特征工程 service
"""
from pyspark.sql import utils
from pyspark.ml.feature import *
from app.ConstFile import const
from app.Utils import *
import app.dao.OperatorDao as OperatorDao
import pyspark.sql.functions as F
import pyspark.sql.types as T

save_dir = const.SAVEDIR


def quantile_discretization(spark_session, operator_id, file_url, condition):
    """
    分位数离散化页面路由
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
        df = read_data(spark_session, file_url)
        # 分位数离散化函数
        result_df = quantile_discretization_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_df.show()
            result_file_url = save_data(result_df)
            run_info = '分位数离散化算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def quantile_discretization_core(df, condition):
    """
    分位数离散化主函数, 将某列连续型数值进行分箱，加入新列；返回df(spark格式)

    :param condition: {"userId":"1","projectName":"订单分析","columnName":"装运成本","newColumnName":"装运成本(分位数离散化)","numBuckets":10}
                        分箱数numBuckets, 默认为5
    :param df:
    :return:
    """

    column_name = condition['columnName']
    # 只能输入一列，否则报错
    if len(column_name.split(",")) != 1:
        return "只能输入一列"
    # 新列的列名默认为columnName + "(分位数离散化)"，若用户指定，以用户指定为准
    try:
        new_column_name = condition['newColumnName']
    except:
        new_column_name = column_name + "(分位数离散化)"
    # 默认分箱数numBuckets为5，若用户指定，以指定为准
    try:
        num_buckets = int(condition['numBuckets'])
    except:
        num_buckets = 5

    # 设定qds（分位数离散化模型）
    qds = QuantileDiscretizer(numBuckets=num_buckets, inputCol=column_name, outputCol=new_column_name,
                              relativeError=0.01, handleInvalid="error")
    # 训练, 输入列必须为数值型，否则返回错误信息
    try:
        df = qds.fit(df).transform(df)
    except utils.IllegalArgumentException:
        return "输入列必须为数值型"
    return df


def vector_indexer(spark_session, operator_id, file_url, condition):
    """
    向量索引转换
    # 向量索引转换旨在转换Vector, 例如：[aa, bb, cc]，而非本例中的单独值，由于没有合适的数据可用，暂时把单独值转换成vector实现功能: aa -> [aa]

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
        df = read_data(spark_session, file_url)
        # 向量索引转换函数
        result_df = vector_indexer_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_df.show()
            result_file_url = save_data(result_df)
            run_info = '向量索引转换化算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def vector_indexer_core(df, condition):
    """
    向量索引转换主函数, 将向量转换成标签索引格式，加入新列；返回df(spark格式)

    :param df:
    :param condition: {"userId":1,"projectId":32,"columnNames":["装运成本"],"newColumnName":"向量索引转换结果","maxCategories":50}
    参数中指定分类标准maxCategories, 默认为20（maxCategories是一个阈值，如[1.0, 2.0, 2.5]的categories是3，小于20，属于分类类型；否则为连续类型）
    :return:
    """
    column_names = condition['columnNames']
    try:
        new_column_name = condition['newColumnName']
    except:
        new_column_name = "_".join(column_names) + "_vectorIndexer"
    # 默认分类阈值maxCategories为20，若用户指定，以指定为准
    try:
        max_categories = int(condition['maxCategories'])
    except:
        max_categories = 20
    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    features_uuid = str(uuid.uuid1())
    vec_assembler = VectorAssembler(inputCols=column_names, outputCol=features_uuid)
    try:
        df = vec_assembler.transform(df)
    except utils.IllegalArgumentException:
        return "输入列必须为数值型"
    # 定义indexer（向量索引转换模型）
    indexer = VectorIndexer(maxCategories=max_categories, inputCol=features_uuid, outputCol=new_column_name)
    # 训练
    df = indexer.fit(df).transform(df)

    # 转换新列的数据格式
    def do_something(col):
        try:
            floatrow = []
            for i in list(col):
                floatrow.append(float(i))
            return floatrow
        except:
            return []

    udf_dosth = F.udf(do_something, T.ArrayType(T.FloatType()))
    df = df.withColumn(new_column_name, udf_dosth(df[new_column_name]))
    df = df.drop(features_uuid)
    return df


def standard_scaler(spark_session, operator_id, file_url, condition):
    """
    标准化列页面路由
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
        df = read_data(spark_session, file_url)
        # 标准化函数
        result_df = standard_scaler_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data(result_df)
            run_info = '标准化算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def standard_scaler_core(df, condition):
    """
    标准化列主函数, 标准化列，使其拥有零均值和等于1的标准差；返回df(spark格式)
    :param df:
    :param condition: {"projectId":32,"columnNames":["利润"],"newColumnName":"利润(标准化)"}
    :return:
    """
    column_names = condition['columnNames']
    # 新列的列名默认为columnName + "(向量索引转换)"，若用户指定，以用户指定为准
    try:
        new_column_name = condition['newColumnName']
    except:
        new_column_name = "_".join(column_names) + "_standardScaler"
    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    features_uuid = str(uuid.uuid1())
    vec_assembler = VectorAssembler(inputCols=column_names, outputCol=features_uuid)
    try:
        df = vec_assembler.transform(df)
    except utils.IllegalArgumentException:
        return "输入列必须为数值型"
    # 设定标准化模型
    standard_scaler_ = StandardScaler(inputCol=features_uuid, outputCol=new_column_name)
    # 训练
    df = standard_scaler_.fit(df).transform(df)

    def do_something(col):
        try:
            floatrow = []
            for i in list(col):
                floatrow.append(float(i))
            return floatrow
        except:
            return []

    udf_dosth = F.udf(do_something, T.ArrayType(T.FloatType()))
    df = df.withColumn(new_column_name, udf_dosth(df[new_column_name]))
    df = df.drop(features_uuid)
    return df


def pca(spark_session, operator_id, file_url, condition):
    """
    降维页面路由
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
        df = read_data(spark_session, file_url)
        # 降维函数
        result_df = pca_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_df.show()
            result_file_url = save_data(result_df)
            run_info = '降维算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def pca_core(df, condition):
    """
    降维主函数, PCA训练一个模型将向量投影到前k个主成分的较低维空间；返回df(spark格式)
    :param df:
    :param condition: {"userId":1,"projectId":32,"columnNames":["销售额","数量","折扣","利润","装运成本"],"newColumnName":"降维结果","k":4}
    :return:
    """
    column_names = condition['columnNames']
    # 新列列名，默认为“降维结果”，若用户指定，以用户指定为准
    try:
        new_column_name = condition['newColumnName']
    except:
        new_column_name = "降维结果"
    # 默认目标维度k为3，若用户指定，以指定为准
    try:
        k = int(condition['k'])
    except:
        k = 3

    # 目标维度k需要小于原始维度，否则返回错误信息
    if k >= len(column_names):
        return "目标维度k必须小于原始维度，才能完成降维，请检查输入的列名和目标维度k（k默认为3）"

    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    features_uuid = str(uuid.uuid1())
    vec_assembler = VectorAssembler(inputCols=column_names, outputCol=features_uuid)
    try:
        df = vec_assembler.transform(df)
    except utils.IllegalArgumentException:
        return "输入列必须为数值型"

    # 设定pca模型
    pca = PCA(k=k, inputCol=features_uuid, outputCol=new_column_name)
    # 训练
    df = pca.fit(df).transform(df)

    # 转换新列的数据格式
    def do_something(col):
        try:
            floatrow = []
            for i in list(col):
                floatrow.append(float(i))
            return floatrow
        except:
            return []

    udf_dosth = F.udf(do_something, T.ArrayType(T.FloatType()))
    df = df.withColumn(new_column_name, udf_dosth(df[new_column_name]))
    df = df.drop(features_uuid)
    return df


def string_indexer(spark_session, operator_id, file_url, condition):
    """
    字符串转标签页面路由
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
        df = read_data(spark_session, file_url)
        # 字符串转标签函数
        result_df = string_indexer_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data(result_df)
            run_info = '字符串转标签算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def string_indexer_core(df, condition):
    """
    # 字符串转标签主函数, 将字符串转换成标签，加入新列；返回df(spark格式)
    # 按label出现的频次，转换成0～分类个数-1，频次最高的转换为0，以此类推；如果输入的列类型为数值型，则会先转换成字符串型，再进行标签化

    :param df:
    :param condition: {"userId":1,"projectId":32,"columnName":"客户名称","newColumnName":"客户名称(标签化，按频率排序，0为频次最高)"}
    :return:
    """
    column_name = condition['columnName']
    # 新列名称，默认为columnName + “(标签化，按频率排序，0为频次最高)”，若用户指定，以用户指定为准
    try:
        new_column_name = condition['newColumnName']
    except:
        new_column_name = column_name + "_string_indexer"
    # 设定si（字符串转标签模型）
    si = StringIndexer(inputCol=column_name, outputCol=new_column_name)
    # 训练
    df = si.fit(df).transform(df)
    return df


def one_hot_encoder(spark_session, operator_id, file_url, condition):
    """
    独热编码页面路由
    :param spark_session:
    :param operator_id:
    :param file_url:
    :param condition:{"userId":1,"projectId":32,"columnNames":["数量","数量"],"newColumnNames":["独热编码1","独热编码2"]}
    :return:
    """

    try:
        # 修改计算状态
        OperatorDao.update_operator_by_id(operator_id, 'running', '', '')
        # 读取数据
        df = read_data(spark_session, file_url)
        # 独热编码函数
        result_df = one_hot_encoder_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data(result_df)
            run_info = '独热编码算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def one_hot_encoder_core(df, condition):
    """
    独热编码主函数, 将一个数值型的列转化成它的二进制形式；返回df(spark格式)
    :param df:
    :param condition:
    :return:
    """
    column_names = condition['columnNames']
    # 新列的列名默认为columnName + "(独热编码)"，若用户指定，以用户指定为准
    try:
        new_column_names = condition['newColumnNames']
    except:
        new_column_names = column_names + "_oneHot"

    # 设定独热编码模型
    one = OneHotEncoderEstimator(inputCols=column_names, outputCols=new_column_names)
    # 训练
    try:
        df = one.fit(df).transform(df)
    except:
        return "只能对数值型的列进行独热编码，且必须为整数或者形如[2.0]的索引，请检查列名输入是否有误"
    return df


def polynomial_expansion(spark_session, operator_id, file_url, condition):
    """
    多项式扩展
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
        df = read_data(spark_session, file_url)
        # 多项式扩展函数
        result_df = polynomial_expansion_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data(result_df)
            run_info = '多项式扩展算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def polynomial_expansion_core(df, condition):
    """
    # 多项式扩展主函数, 将n维的原始特征组合扩展到多项式空间；返回df(spark格式)
    # 功能说明：以degree为2的情况为例，输入为(x,y), 则输出为(x, x * x, y, x * y, y * y)
    # 为了更加直观，举个带数字的例子，[0.5, 2.0] -> [0.5, 0.25, 2.0, 1.0, 4.0]
    :param df:
    :param condition: {"projectId":32,"columnNames":["数量","折扣","装运成本"],"newColumnName":"多项式扩展"}
    :return:
    """
    column_names = condition['columnNames']
    # 新列的列名默认为"多项式扩展" + columnNames，若用户指定，以用户指定为准
    try:
        new_column_name = condition['newColumnName']
    except:
        new_column_name = "_".join(column_names) + "_PolynomialExpansion"
    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    features_uuid = str(uuid.uuid1())
    vec_assembler = VectorAssembler(inputCols=column_names, outputCol=features_uuid)
    try:
        df = vec_assembler.transform(df)
    except utils.IllegalArgumentException:
        return "输入列必须为数值型"
    # 设定多项式扩展模型
    px = PolynomialExpansion(inputCol=features_uuid, outputCol=new_column_name)
    # 训练
    df = px.transform(df)

    # 转换新列的数据格式
    def do_something(col):
        try:
            floatrow = []
            for i in list(col):
                floatrow.append(float(i))
            return floatrow
        except:
            return []

    udf_dosth = F.udf(do_something, T.ArrayType(T.FloatType()))
    df = df.withColumn(new_column_name, udf_dosth(df[new_column_name]))
    df = df.drop(features_uuid)
    return df


def chiSqSelector(spark_session, operator_id, file_url, condition):
    """
    卡方选择
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
        df = read_data(spark_session, file_url)
        # 卡方选择函数
        result_df = chiSqSelector_core(df, condition)
        if isinstance(result_df, str):
            OperatorDao.update_operator_by_id(operator_id, 'error', '', result_df)
        else:
            # 存储结果
            result_file_url = save_data(result_df)
            run_info = '卡方选择算子执行成功'
            # 修改计算状态
            OperatorDao.update_operator_by_id(operator_id, 'success', result_file_url, run_info)
            return [result_file_url]

    except Exception as e:
        run_info = str(e)
        OperatorDao.update_operator_by_id(operator_id, 'error', '', run_info)
        traceback.print_exc()
    return []


def chiSqSelector_core(df, condition):
    """
    # 卡方选择主函数, 在特征向量中选择出那些“优秀”的特征，组成新的、更“精简”的特征向量；返回df(spark格式)
    # 对特征和真实标签label之间进行卡方检验，来判断该特征和真实标签的关联程度，进而确定是否对其进行选择
    # 对于label的选择，理论上来说要选择机器学习的目标列（若该列不是数值型，需将其数值化），但是无相应数据，本例中选择label="数量"，无实际意义
    :param df:
    :param condition: {"userId":"1","projectId":"订单分析","columnNames":["折扣","装运成本"],"columnName_label":"数量","newColumnName":"卡方选择","numTopFeatures":2}
    :return:
    """
    column_names = condition['columnNames']
    column_name_label = condition['columnName_label']
    # columnName_label必须为单列，否则报错
    if len(column_name_label.split(",")) != 1:
        return "卡方基准列label只能选择一列，请检查列名输入是否有误"

    # 获取卡方选择结果topN的数目，默认numTopFeatures为1
    try:
        num_top_features = condition['numTopFeatures']
    except:
        num_top_features = 1

    # columnNames的数目必须大于1，否则报错
    if len(column_names) < 2:
        return "用于卡方选择的列必须大于1，请检查列名输入是否有误"
    # 新列的列名默认为"卡方选择" + (与 columnName_label 相关的前 numTopFeatures 个特征列)，若用户指定，以用户指定为准
    try:
        new_column_name = condition['newColumnName']
    except:
        new_column_name = "卡方选择" + "(与 [" + str(column_name_label) + "] 相关的前 " + str(num_top_features) + " 个特征列)"

    # 转化列类型 -> 向量, 输入列必须为数值型，否则返回错误信息
    features_uuid = str(uuid.uuid1)
    vec_assembler = VectorAssembler(inputCols=column_names, outputCol=features_uuid)
    try:
        df = vec_assembler.transform(df)
    except utils.IllegalArgumentException:
        return "选择的列（包括用于卡方选择的列和卡方基准列label）都必须为数值型，请检查列名输入是否有误(Tip:feature)"

    # 设定标签列label
    label_uuid = str(uuid.uuid1())
    df = df.withColumn(label_uuid, df[column_name_label])
    # 设定卡方选择模型
    selector = ChiSqSelector(numTopFeatures=num_top_features, featuresCol=features_uuid, outputCol=new_column_name,
                             labelCol=label_uuid, )
    # 训练，若label的类型不是数值型，报错
    try:
        df = selector.fit(df).transform(df)
    except utils.IllegalArgumentException:
        return "选择的列（包括用于卡方选择的列和卡方基准列label）都必须为数值型，请检查列名输入是否有误(Tip:label)"

    # 转换新列的数据格式
    def do_something(col):
        try:
            floatrow = []
            for i in list(col):
                floatrow.append(float(i))
            return floatrow
        except:
            return []

    udf_dosth = F.udf(do_something, T.ArrayType(T.FloatType()))
    df = df.withColumn(new_column_name, udf_dosth(df[new_column_name]))
    df = df.drop(features_uuid)
    if not (column_name_label == label_uuid):
        df = df.drop(label_uuid)
    return df
