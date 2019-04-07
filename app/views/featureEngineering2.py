# -*- coding: UTF-8 -*-
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, abort
from flask.json import jsonify
from app import app
import json
import os
import time
from app.utils import mkdir, getProjectCurrentDataUrl, is_number, addProcessingFlow
import app.utils as apus
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, concat_ws, regexp_replace
import random
import string

# csv文件存储目录（临时）
save_dir = "/home/zk/project/test.csv"
# save_dir = '/Users/kang/PycharmProjects/project/test.csv'

#解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)
app.response_class = MyResponse

