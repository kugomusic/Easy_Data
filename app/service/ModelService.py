# -*- coding: UTF-8 -*-

import app.dao.ModelDao as ModelDao
import app.dao.OperatorDao as OperatorDao
import app.dao.OperatorTypeDao as OperatorTypeDao
from app.models.Mysql import Operator
import app.service.ModelExecuteService as ModelExecuteService
from app.Utils import *

"""
关于model（执行流程）的处理方法
"""


def update_model(project_id, start_nodes, config, relationship, config_order):
    """
    更新 model (处理流程图)
    ToDo: 没有考虑数据库操作的原子性
    :param project_id:
    :param start_nodes:
    :param config:
    :return:
    """
    # 获取model
    model = ModelDao.get_model_by_project_id(project_id)
    if model is False:
        return False

    # 更新model
    update_result = ModelDao.update_with_project_id(project_id, start_nodes, relationship, config_order)
    if update_result is False:
        return False

    # 删除旧的operator
    delete_result = OperatorDao.delete_operator_by_model_id(model.id)
    if delete_result is False:
        return False

    # 添加新的operator
    operators = []
    config_dict = json.loads(config)
    for operator_id in config_dict.keys():
        operator_dict = config_dict.get(operator_id)
        operator_style = json.dumps({'location': operator_dict['location'], }, ensure_ascii=False)
        # json.dumps(operates, ensure_ascii=False)
        ope = Operator(id=operator_id,
                       father_operator_ids=','.join(operator_dict['pre']),
                       child_operator_ids=','.join(operator_dict['next']),
                       model_id=model.id,
                       status='initial',
                       operator_type_id=operator_dict['name'],
                       operator_config=json.dumps(operator_dict['config'], ensure_ascii=False),
                       operator_style=operator_style)
        operators.append(ope)

    create_result = OperatorDao.create_operator(operators)
    if create_result is False:
        return False

    return True


def get_model_by_project_id(project_id):
    """
    获取项目对应的model(执行流程)
    :param project_id:
    :return:
    """
    # 获取 model
    model = ModelDao.get_model_by_project_id(project_id)
    if model is False:
        return False

    # 获取 operator
    operators = OperatorDao.get_operator_by_model_id(model.id)
    if operators is False:
        return False

    # 获取 operator_type
    operator_types = OperatorTypeDao.get_all_operator_type()
    if operator_types is False:
        return False

    # TODO : 查询数据源表

    operator_types_dict = dict()
    for operator_type in operator_types:
        operator_types_dict[operator_type.id] = operator_type

    # 返回结果
    config = dict()
    for operator in operators:
        config[operator.id] = {'type': operator_types_dict[operator.operator_type_id].type_name,
                               'name': operator_types_dict[operator.operator_type_id].id,
                               'location': json.loads(operator.operator_style)['location'],
                               'config': json.loads(operator.operator_config),
                               'next': operator.child_operator_ids.split(','),
                               "pre": operator.father_operator_ids.split(',')}
    model_config = json.loads(model.config)
    relationship = []
    for item in model_config['relationship'].split('*,'):
        relationship.append(list_str_to_list(item))
    config_order = json.loads(model_config['config_order'])
    return {'projectId': project_id, 'config': config, 'startNode': model.start_nodes.split(','),
            'relationship': relationship, 'config_order': config_order}


def get_run_status_by_project_id(project_id, operator_id=None):
    """
    获取某次执行的状态和其中的每个算子的状态

    :param project_id:
    :param operator_id: 从某个算子开始 此次执行的整体状态 。如果是空 ，默认查询整个model的执行状态
    :return:
    """

    # 获取 model
    model = ModelDao.get_model_by_project_id(project_id)
    if model is False:
        return False

    # 获取 operator
    operators = OperatorDao.get_operator_by_model_id(model.id)
    if operators is False:
        return False

    # operator_status返回结果
    result = dict()
    for operator in operators:
        result[operator.id] = {"status": operator.status, "log": operator.run_info}

    # 构造dict
    id_operator_dict = {}
    for operator in operators:
        id_operator_dict[operator.id] = operator

    status_set = set()

    # queue
    if operator_id is None:
        operator_from_one_ids = model.start_nodes.split(',')
    else:
        operator_from_one_ids = [operator_id]

    # 12222
    while operator_from_one_ids:
        item = operator_from_one_ids.pop(0)
        if not (item is None or item == ''):
            status_set.add(id_operator_dict[item].status)
            operator_from_one_ids.extend(operator.child_operator_ids.split(','))

    """
    有running，model running 
    没有running，有error, model error
    全部success，model success
    
    """

    for operator in operators:
        status_set.add(operator.status)

    if 'running' in status_set:
        model_execute_status = 'running'
    elif 'error' in status_set:
        # TODO:多棵树的时候 有的树是error 有的是initial 可能存在这种情况
        model_execute_status = 'error'
    elif ('initial' in status_set) and ('success' not in status_set):
        model_execute_status = 'initial'
    elif ('initial' in status_set) and ('success' in status_set):
        model_execute_status = 'running'
    else:
        model_execute_status = 'success'

    return {"modelExecuteStatus": model_execute_status, "operatorStatus": result}


def model_execute_from_start(user_id, project_id):
    """
    执行模型
    :param user_id: 1
    :param project_id: 32
    :return:
    """
    # 获取 model
    model = ModelDao.get_model_by_project_id(project_id)
    if model is False:
        return False

    # spark会话
    spark_session = getSparkSession(user_id, "executeAll")

    # 多线程执行
    start_nodes = model.start_nodes.split(',')
    print("-----model_execute_from_start------", "start_nodes", ','.join(start_nodes))
    ModelExecuteService.model_thread_execute(spark_session, start_nodes)


def model_execute_from_one(user_id, operator_id):
    """
    执行模型
    :param user_id: 1
    :param operator_id: 1
    :return:
    """

    # spark会话
    spark_session = getSparkSession(user_id, "executeFromOne")

    # 多线程执行
    start_nodes = [operator_id]
    print("-----model_execute_from_one------", "start_nodes", ','.join(start_nodes))
    ModelExecuteService.model_thread_execute(spark_session, start_nodes)
