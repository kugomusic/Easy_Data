from app import db
from app import app
import click

# 创建数据库
# @app.cli.command()
def initdb():
    db.create_all()
    click.echo('Initialized database.')

# 删除数据库
# @app.cli.command()
def dropdb():
    db.drop_all()
    click.echo('Drop database.')

# 数据源类
class DataSource(db.Model):
    __tablename__ = 'data_source'
    id = db.Column(db.Integer, primary_key=True)
    file_name = db.Column(db.String(64), unique=True, index=True)
    file_url = db.Column(db.Text, unique=True)
    file_type = db.Column(db.String(64))


# 用户类
class User(db.Model):
    __tablename__ = 'user'
    id = db.Column(db.Integer, primary_key=True)
    user_name = db.Column(db.String(64), unique=True, index=True)
    password = db.Column(db.String(128))

# 项目类
class Project(db.Model):
    __tablename__ = 'project'
    id = db.Column(db.Integer, primary_key=True)
    project_name = db.Column(db.String(64), unique=True, index=True)
    project_address = db.Column(db.String(256), unique=True, index=True)
    user_id = db.Column(db.Integer, db.ForeignKey('user.id'))
    dataSource_id = db.Column(db.Integer, db.ForeignKey('data_source.id'))
