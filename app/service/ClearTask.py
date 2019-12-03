# -*- coding: UTF-8 -*-
from app.models.MSEntity import Operator, MLModel
from app import db
from app.Utils import deltree, deldir

"""
该类的作用：清理无用的中间数据
"""

import os

filePath = '/home/zk/midData'
modelPaths = ['/home/zk/midData/model/secondClassification/svm', '/home/zk/midData/model/secondClassification/gbdt']

# 数据可用到的中间数据
urls_arr = []
# 查找operator
query = db.session.query(Operator)
db.session.commit()
for operator in query:
    url = operator.operator_output_url
    if (url is not None) and (url is not ''):
        urls_arr.extend(url.split('*,'))

# 查找 保存的model
models = db.session.query(MLModel)
db.session.commit()
for model in models:
    url = model.model_url
    if (url is not None) and (url is not ''):
        urls_arr.extend(url.split('*,'))
print(urls_arr)

# 磁盘上所有中间数据
all_file = []
for i, j, k in os.walk(filePath):
    for item in k:
        all_file.append(filePath + '/' + item)
    break

for modelPath in modelPaths:
    for i, j, k in os.walk(modelPath):
        for item in j:
            all_file.append(modelPath + '/' + item)
        break

for url in all_file:
    print(url)

print('******删除一下内容：')
for url in all_file:
    if url not in urls_arr:
        print("删除：" + url)
        if os.path.isdir(url):
            deltree(url)
        elif os.path.isfile(url):
            deldir(url)
