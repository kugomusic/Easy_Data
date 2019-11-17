# -*- coding: UTF-8 -*-
from flask import jsonify, Response
from app import app
from app.Utils import *
import app.dao.OperatorTypeDao as OperatorTypeDao


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


@app.route('/operateType/getAll', methods=['GET', 'POST'])
def get_all_operate_type():
    """
    获取所有的算子种类
    :return:
    """
    operator_types = OperatorTypeDao.get_all_operator_type()

    aaa = dict()
    for i in operator_types:
        if i.type_label not in aaa.keys():
            aaa[i.type_label] = [{"id": i.id, "name": i.type_name}]
        else:
            aaa.get(i.type_label).append({"id": i.id, "name": i.type_name})
            # 和Java一样 存的是数组的引用呀

    item_list = []
    for name in aaa.keys():
        list = aaa.get(name)
        item_list.append({'name': name, 'list': list})

    result = {'list': item_list}
    return result
