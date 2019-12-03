# -*- coding: UTF-8 -*-
from flask import jsonify, Response, request
from app import app
from app.Utils import *
import app.dao.OperatorDao as OperatorDao
import pandas as pd
import app.service.MLModelService as MLModelService


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


@app.route('/operate/getOperateResultData', methods=['GET', 'POST'])
def get_operate_result_data():
    """
    查看算子运行结果数据
    :return:
    """
    operator_id = request.form.get('operatorId')
    start = int(request.form.get('start'))
    end = int(request.form.get('end'))
    print(operator_id, start, end)
    operator = OperatorDao.get_operator_by_id(operator_id)
    if operator.status != "success":
        return "请执行该节点"
    if operator.operator_output_url is not None:
        operator_output_url = operator.operator_output_url.split('*,')
    else:
        return "没有运行结果"
    result_arr = []
    try:
        for i in range(len(operator_output_url)):
            data = pd.read_csv(operator_output_url[i], encoding='utf-8')
            if len(data) < end:
                end = len(data)
            if start > end:
                result_arr.append({'length': len(data), 'data': "请输入合法参数", 'position': i})
            else:
                data2 = data[int(start):int(end)].to_json(orient='records', force_ascii=False)
                result_arr.append({'length': len(data), 'data': json.loads(data2), 'position': i})
        return jsonify(result_arr)
    except:
        traceback.print_exc()
        return "Error，please contact the administrator "


@app.route('/operate/saveOperateModel', methods=['GET', 'POST'])
def save_operate_model():
    """
    对于模型算子 保存模型
    :return:
    """
    operator_id = request.form.get('operatorId')
    user_id = request.form.get('userId')
    name = request.form.get('name')

    try:
        result = MLModelService.save_ml_model(operator_id, user_id, name)
        if isinstance(result, str):
            return result
        if isinstance(result, bool):
            if result is True:
                return "success"
        return "fail"
    except:
        traceback.print_exc()
        return "Error，please contact the administrator "


@app.route('/operate/getOperateModel', methods=['GET', 'POST'])
def get_operate_model():
    """
    获取保存的模型
    :return:
    """
    ml_model_id = request.args.get('MLModelId')
    project_id = request.args.get('projectId')
    user_id = request.args.get('userId')
    model_id = request.args.get('modelId')
    name = request.args.get('name')
    status = request.args.get('status')

    print(ml_model_id, project_id, user_id, model_id, name, status)
    try:
        results = MLModelService.get_ml_model(ml_model_id, project_id, user_id, model_id, name, status)
        return jsonify(results)
    except:
        traceback.print_exc()
    return "Error，please contact the administrator "


@app.route('/operate/deleteOperateModel', methods=['GET', 'POST'])
def delete_operate_model():
    """
    删除保存的模型
    :return:
    """
    ml_model_id = request.form.get('MLModelId')
    try:
        model = MLModelService.delete_ml_model(ml_model_id)
        return 'success'
    except:
        traceback.print_exc()
    return "Error，please contact the administrator "
