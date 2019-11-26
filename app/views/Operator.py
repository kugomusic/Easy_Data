# -*- coding: UTF-8 -*-
from flask import jsonify, Response, request
from app import app
from app.Utils import *
import app.dao.OperatorDao as OperatorDao
import pandas as pd


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
