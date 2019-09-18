# -*- coding: UTF-8 -*-

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
# 跨域 方法一 ：falsk_cors模块
# from flask_cors import CORS
# CORS(app, supports_credentials=True)
# 跨域支持 方法二：flask 内置的after_request()方法
def after_request(resp):
    resp.headers['Access-Control-Allow-Origin'] = '*'
    return resp
app.after_request(after_request)

# 加载配置文件
app.config.from_object('config')
db = SQLAlchemy(app)
#
from app.views import mysql, exploration, process,process2, operateFlow, featureEngineering

# from app import test