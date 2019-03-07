import sys

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

app = Flask(__name__)

app.config.from_object('config') # 加载配置文件
db = SQLAlchemy(app)

from app import routers