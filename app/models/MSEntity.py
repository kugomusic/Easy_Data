# -*- coding: UTF-8 -*-
from app import db
import click


# @app.cli.command()
def initdb():
    """
    创建数据库
    :return:
    """
    db.create_all()
    click.echo('Initialized database.')


# @app.cli.command()
def dropdb():
    """
    删除数据库
    :return:
    """
    db.drop_all()
    click.echo('Drop database.')


class DataSource(db.Model):
    """
    数据源类
    """
    __tablename__ = 'data_source'
    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String(64), unique=True, index=True)
    file_url = db.Column(db.Text, unique=True)
    file_type = db.Column(db.String(64))
    create_user = db.Column(db.String(64))
    open_level = db.Column(db.String(64))
    create_time = db.Column(db.String(64))


class User(db.Model):
    """
    用户类
    """
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    user_name = db.Column(db.String(64), unique=True, index=True)
    password = db.Column(db.String(128))


class Project(db.Model):
    """
    项目类
    """
    __tablename__ = 'project'
    id = db.Column(db.Integer, primary_key=True)
    project_name = db.Column(db.String(64), unique=True, index=True)
    project_address = db.Column(db.String(256), unique=True, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    dataSource_id = db.Column(db.Integer, db.ForeignKey('data_source.id'))


class ProcessFlow(db.Model):
    """
    数据处理流程类
    """
    __tablename__ = 'process_flow'
    id = db.Column(db.Integer, primary_key=True)
    project_id = db.Column(db.String(64), unique=True, index=True)
    operates = db.Column(db.String(13000))
    cur_ope_id = db.Column(db.String(128))
    links = db.Column(db.String(5000))


class Model(db.Model):
    """
    DAG图模型类
    """
    __tablename__ = 'model'
    id = db.Column(db.Integer, primary_key=True)
    model_name = db.Column(db.String(64))
    project_id = db.Column(db.Integer)
    start_nodes = db.Column(db.String(2048))
    config = db.Column(db.String(8192))
    create_time = db.Column(db.String(32))


class OperatorType(db.Model):
    """
    算子的类型（如：读数据，filter）
    """
    __tablename__ = 'operator_type'
    id = db.Column(db.Integer, primary_key=True)
    type_name = db.Column(db.String(128))
    type_label = db.Column(db.String(128))


class Operator(db.Model):
    """
    算子类
    """
    __tablename__ = 'operator'
    id = db.Column(db.String(128), primary_key=True)
    operator_name = db.Column(db.String(64))
    father_operator_ids = db.Column(db.String(128))
    child_operator_ids = db.Column(db.String(128))
    model_id = db.Column(db.Integer)
    status = db.Column(db.String(32))
    operator_output_url = db.Column(db.String(512))
    operator_input_url = db.Column(db.String(512))
    operator_type_id = db.Column(db.Integer)
    operator_config = db.Column(db.String(4096))
    operator_style = db.Column(db.String(4096))
    run_info = db.Column(db.String(8192))


class ModelExecute(db.Model):
    """
    模型执行记录表
    """
    __tablename__ = 'model_execute'
    id = db.Column(db.Integer, primary_key=True)
    model_id = db.Column(db.Integer)
    start_nodes = db.Column(db.String(2048))
    status = db.Column(db.String(32))
    execute_user_id = db.Column(db.Integer)
    run_info = db.Column(db.String(4096))
    create_time = db.Column(db.String(32))
    end_time = db.Column(db.String(32))


class Report(db.Model):
    """
    报告表
    """
    __tablename__ = 'report'
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.String(128))
    report_title = db.Column(db.String(128))
    report_content = db.Column(db.String(20000))
