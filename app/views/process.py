# -*- coding: UTF-8 -*-
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, abort
from flask.json import jsonify
from app import app
import json
import os
import time
from app.utils import mkdir,getProjectCurrentDataUrl
import pandas as pd
from pyspark.sql import SparkSession
import random
import string

#解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)

app.response_class = MyResponse

# 获取项目对应的当前数据源的所有列名
@app.route("/dataProcess", methods=['GET','POST'])
def dataProcess():
    projectName = request.form.get('projectName')
    projectName = request.form.get('doubleName')


def filter(parameter):
    spark = SparkSession \
        .builder \
        .appName("Python Spark SQL basic example") \
        .config("spark.some.config.option", "some-value") \
        .getOrCreate()
    fileUrl = parameter['fileUrl']
    condition = parameter['condition']
    tableName = ''.join(random.sample(string.ascii_letters + string.digits, 8))
    conStr = 'select * from '+tableName +' where '
    for i in condition:
        conStr = conStr + i['name'] + i['operate'] + i['value'] + i['relation']
    df = spark.read.csv(fileUrl)

    df.createOrReplaceTempView(tableName)
    spark.sql('')
    df.filter(conStr)