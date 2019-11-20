# -*- coding: utf-8 -*-
import os
from flask import request, send_from_directory, Response
from app.views.datasource.werkzeug.utils import secure_filename
from app import app, db
from app.models.Mysql import DataSource
from sqlalchemy.sql import and_, or_, text

from flask.json import jsonify
import json
import pandas as pd

ALLOWED_EXTENSIONS = set(['txt', 'csv', 'pdf', 'png', 'jpg', 'jpeg', 'gif'])

app.config['UPLOAD_FOLDER'] = os.getcwd() + '/../data'
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024 * 1024

html = '''
    <!DOCTYPE html>
    <title>Upload File</title>
    <h1>文件上传</h1>
    <form method=post enctype=multipart/form-data>
         <input type=file name=file>
         <input type=submit value=上传>
    </form>
    '''


# 解决 list, dict 不能返回的问题
class MyResponse(Response):
    @classmethod
    def force_type(cls, response, environ=None):
        if isinstance(response, (list, dict)):
            response = jsonify(response)
        return super(Response, cls).force_type(response, environ)


app.response_class = MyResponse


def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1] in ALLOWED_EXTENSIONS


@app.route('/uploads/<filename>')
def uploaded_file(filename):
    return send_from_directory(app.config['UPLOAD_FOLDER'], filename)


@app.route('/dataSource/upload', methods=['GET', 'POST'])
def upload_file():
    """
    上传文件
    :return:
    """
    if request.method == 'POST':
        file = request.files['file']
        # create_user = request.form.get('userId')
        # open_level = request.form.get('openLevel')
        create_user = 1
        open_level = 1
        if file and allowed_file(file.filename):
            # 保存文件
            from unicodedata import normalize
            filename = secure_filename(normalize('NFKD', file.filename).encode('utf-8', 'ignore').decode('utf-8'))
            print('文件保存路径：', os.path.join(app.config['UPLOAD_FOLDER'], filename))
            file.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            # file_url = url_for('uploaded_file', filename=filename)
            file_url = os.path.join(app.config['UPLOAD_FOLDER'], filename)

            # 保存文件记录
            import time
            # 格式化成2016-03-20 11:45:39形式
            create_time = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
            add_DataSource(file.filename[:-4], file_url, "CSV", create_user, open_level, create_time)
            return '文件上传成功'
    return html


@app.route('/dataSource/getFiles', methods=['GET', 'POST'])
def get_file():
    """
    查询数据源列表
    :return:
    """
    # 多条件并集查询
    condition = (DataSource.open_level == '0')  # 公开的数据集
    # 根据条件筛选酒店信息
    if request.args.get('userId'):  #
        userId = request.args.get('userId')
        condition = or_(condition, DataSource.create_user == userId)

    # 排序方式
    order_string = 'create_time' + ' desc'
    dataSource = DataSource.query.filter(condition).order_by(text(order_string))  # 执行查询

    result = []
    for i in dataSource:
        result.append({"fileId": i.id, "fileName": i.file_name, 'fileUrl': i.file_url, 'fileType': i.file_type,
                       'createUser': i.create_user, 'openLevel': i.open_level, 'createTime': i.create_time})
    return jsonify(result)


def add_DataSource(file_name, file_url, file_type, create_user, open_level, create_time):
    """
    新增数据源
    :param file_name:
    :param file_url:
    :param file_type:
    :param create_user:
    :param open_level:
    :param create_time:
    :return:
    """
    source = DataSource(file_name=file_name, file_url=file_url, file_type=file_type, create_user=create_user,
                        open_level=open_level, create_time=create_time)
    db.session.add(source)
    db.session.commit()


@app.route('/dataSource/dataPreview', methods=['GET', 'POST'])
def data_preview():
    """
    查看数据
    :return:
    """
    if request.method == 'POST':
        userId = request.form.get('userId')
        fileId = request.form.get('fileId')
        fileUrl = request.form.get('fileUrl')
        start = request.form.get('start')
        end = request.form.get('end')

    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        data2 = data[int(start):int(end)].to_json(orient='records', force_ascii=False)
        return jsonify({'length': len(data), 'data': json.loads(data2)})
    except:
        return "error read"


@app.route("/dataSource/getColumnNames", methods=['GET', 'POST'])
def get_column_names():
    """
    获取项目对应的当前数据源的所有列名
    :return:
    """
    if request.method == 'GET':
        userId = request.args.get('userId')
        fileId = request.args.get('fileId')
        fileUrl = request.args.get('fileUrl')
    else:
        return "需要get请求"

    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        return jsonify(data.columns.values.tolist())
    except:
        return "error read"


@app.route("/dataSource/getColumnNameWithNumberType", methods=['GET', 'POST'])
def get_column_names_with_number_type():
    """
    获取项目对应的当前数据源的 数值型 的列名
    :return:
    """
    if request.method == 'GET':
        userId = request.args.get('userId')
        fileId = request.args.get('fileId')
        fileUrl = request.args.get('fileUrl')
    else:
        return "需要get请求"
    try:
        data = pd.read_csv(fileUrl, encoding='utf-8')
        res = []
        for col in data.columns.values.tolist():
            if (data[col].dtype == 'int64' or data[col].dtype == 'float64'):
                res.append(col)
        return jsonify(res)
    except:
        return "error read"


def data_read(session, type, url):
    """
    读取数据
    :param session: Spark Session
    :param type: 数据源类型
    :param url: 数据源地址
    :return Spark DataFrame:
    """
    try:
        # type = 'CSV'
        df = session.read.csv(url, header=True, inferSchema=True)
        return df
    except:
        return "error"
