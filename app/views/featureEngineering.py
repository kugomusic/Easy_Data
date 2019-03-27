# -*- coding: UTF-8 -*-
from flask import request, jsonify, Response
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
from pyspark.mllib.feature import *


# 特征工程开始施工...