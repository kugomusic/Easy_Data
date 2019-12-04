# -*- coding: UTF-8 -*-

import app.dao.ModelDao as ModelDao
import app.dao.OperatorDao as OperatorDao
import app.dao.OperatorTypeDao as OperatorTypeDao
import app.dao.ModelExecuteDao as ModelExecuteDao
from app.models.MSEntity import Operator, ModelExecute
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
    try:
        # 获取model
        model = ModelDao.get_model_by_project_id(project_id)
        if model is False:
            return False

        # 更新model
        update_result = ModelDao.update_with_project_id(project_id, start_nodes, relationship, config_order)
        if update_result is False:
            return False

        # 获取 operator
        operator_old = OperatorDao.get_operator_by_model_id(model.id)

        # 新的operator
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

        # 准备删除的算子
        operator_delete = []
        # 准备更新的算子
        operator_update = []
        for old in operator_old:
            flag_exist = False
            for new in operators:
                if old.id == new.id:
                    flag_exist = True
                    operator_update.append([old, new])
                    break
            if not flag_exist:
                operator_delete.append(old)
        # 删除算子
        for delete in operator_delete:
            OperatorDao.delete_operator_by_id(delete.id)
            print("********删除算子", delete)

        # 更新算子
        for update in operator_update:
            update[0].father_operator_ids = update[1].father_operator_ids
            update[0].child_operator_ids = update[1].child_operator_ids
            update[0].model_id = update[1].model_id
            update[0].operator_type_id = update[1].operator_type_id
            update[0].operator_config = update[1].operator_config
            update[0].operator_style = update[1].operator_style
            # 更新算子
            OperatorDao.create_operator([update[0]])
            print("*********更新算子", update[0])

        # 准备添加的算子
        operator_add = []
        for new in operators:
            flag_exist = False
            for old in operator_old:
                if old.id == new.id:
                    flag_exist = True
                    break
            if not flag_exist:
                operator_add.append(new)
        # 添加算子
        OperatorDao.create_operator(operator_add)
        print("*********添加算子", operator_add)
        return True
    except:
        traceback.print_exc()
        return False


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


def get_status_model_execute_end(project_id, start_operator_ids):
    """
    获取运行结束后的状态

    :param project_id:
    :param start_operator_ids:
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

    # 构造dict
    id_operator_dict = {}
    for operator in operators:
        id_operator_dict[operator.id] = operator

    operator_from_one_ids = []
    operator_from_one_ids.extend(start_operator_ids)

    # 从此次执行 起始节点及以后节点的状态
    status_set = set()
    while operator_from_one_ids:
        item = operator_from_one_ids.pop(0)
        if not (item is None or item == ''):
            status_set.add(id_operator_dict[item].status)
            operator_from_one_ids.extend(id_operator_dict[item].child_operator_ids.split(','))

    if len(status_set) == 1 and "success" in status_set:
        return "success"
    else:
        return "error"


def get_run_status_by_project_id(project_id, model_execute_id):
    """
    获取某次执行的状态和其中的每个算子的状态

    :param project_id:
    :param model_execute_id: model的执行记录ID
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

    # 构造dict
    id_operator_dict = {}
    for operator in operators:
        id_operator_dict[operator.id] = operator

    # 查看此次执行记录（状态、起始节点）
    model_execute_ = ModelExecuteDao.get_model_execute_by_id(model_execute_id)
    operator_from_one_ids = model_execute_.start_nodes.split(',')

    # 查看此次执行的所有节点的状态
    result = dict()
    while operator_from_one_ids:
        item = operator_from_one_ids.pop(0)
        if not (item is None or item == ''):
            result[id_operator_dict[item].id] = {"status": id_operator_dict[item].status,
                                                 "log": id_operator_dict[item].run_info}
            operator_from_one_ids.extend(id_operator_dict[item].child_operator_ids.split(','))

    return {"modelExecuteStatus": model_execute_.status, "operatorStatus": result}


def run_execute_status_from_start(user_id, project_id):
    """
    设置模型运行时状态(从头开始执行)
    :param user_id:
    :param project_id:
    :return:
    """
    # 获取 model
    model = ModelDao.get_model_by_project_id(project_id)
    if model is False:
        return False
    # 状态初始化
    start_nodes = model.start_nodes.split(',')
    model_execute_id = initial_execute_status(user_id, start_nodes)
    ModelExecuteDao.update_model_execute(model_execute_id, "running", "", "")
    return {'model_execute_id': model_execute_id, 'start_nodes': start_nodes}


def run_execute_status_from_one(user_id, operator_id):
    """
    设置模型运行时状态（从某个节点开始执行）
    :param user_id:
    :param operator_id:
    :return:
    """
    # 状态初始化
    start_nodes = [operator_id]
    model_execute_id = initial_execute_status(user_id, start_nodes)
    ModelExecuteDao.update_model_execute(model_execute_id, "running", "", "")
    return {'model_execute_id': model_execute_id, 'start_nodes': start_nodes}


def model_execute(user_id, project_id, param):
    """
    执行模型
    :param user_id: 1
    :param project_id: 32
    :param param: {'model_execute_id': model_execute_id, 'start_nodes': start_nodes}
    :return:
    """
    model_execute_id = param['model_execute_id']
    start_nodes = param['start_nodes']
    # spark会话
    spark_session = getSparkSession(user_id, "executeModel")
    # 多线程执行
    print("-----model_execute_from_start------", "start_nodes", ','.join(start_nodes))
    ModelExecuteService.model_thread_execute(spark_session, start_nodes)
    # 执行完毕，更改执行状态
    end_status = get_status_model_execute_end(project_id, start_nodes)
    ModelExecuteDao.update_model_execute(model_execute_id, end_status, "",
                                         time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    return model_execute_id


def initial_execute_status(execute_user_id, start_nodes):
    """
    每次执行model时，初始化执行状态
    :param execute_user_id:
    :param start_nodes: []
    :return:
    """
    # 查找参与运行的 operator
    operator_list = []
    operator_id_queue = []
    for x in start_nodes:
        operator_id_queue.append(x)
    while len(operator_id_queue) > 0:
        operator_id = operator_id_queue.pop(0)
        if operator_id is None or operator_id == "":
            continue
        operator = OperatorDao.get_operator_by_id(operator_id)
        operator_list.append(operator)
        for x in operator.child_operator_ids.split(','):
            operator_id_queue.append(x)

    # 每个operator 状态初始化为initial
    for operator in operator_list:
        OperatorDao.update_operator_by_id(operator.id, "initial")

    # 追加执行记录
    model_execute = ModelExecute(start_nodes=','.join(start_nodes), status='initial', execute_user_id=execute_user_id,
                                 create_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
    model_execute = ModelExecuteDao.create_model_execute(model_execute)
    if model_execute is False:
        return False
    else:
        return model_execute.id
