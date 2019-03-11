from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)
# 跨域
from flask_cors import CORS
CORS(app, supports_credentials=True)
# 加载配置文件
app.config.from_object('config')
db = SQLAlchemy(app)

from app.views import mysql
from app.views import exploration
from app import test