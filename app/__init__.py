# -*- coding: UTF-8 -*-

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
# 跨域
from flask_cors import CORS
CORS(app, supports_credentials=True)
# 加载配置文件
app.config.from_object('config')
db = SQLAlchemy(app)
#
from app.views import mysql, exploration, process,process2, operateFlow, featureEngineering

# from app import test