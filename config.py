# -*- coding: UTF-8 -*-
import os

# 24位字符设置
SECRET_KEY = os.urandom(24)

# 这里登陆的是root用户，要填上自己的密码，MySQL的默认端口是3306，填上之前创建的数据库名test
# SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:mySQL#h@d00p@localhost:3306/visualization'
SQLALCHEMY_DATABASE_URI = 'mysql+pymysql://root:0000@localhost:3306/visualization'

# 设置这一项是每次请求结束后都会自动提交数据库中的变动
SQLALCHEMY_TRACK_MODIFICATIONS = False
