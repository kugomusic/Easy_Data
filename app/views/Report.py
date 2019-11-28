# encoding=utf8
import sys
from importlib import reload

from flask import request, jsonify, Response
from flask.json import jsonify
from app import app
from app import db
from app.models.MSEntity import DataSource, Project, Report
import traceback
from app.Utils import mkdir, getProjectByNameAndUserId
from app.ConstFile import const


@app.route('/report/getAll', methods=['GET', 'POST'])
def report_get_all():
    """
    获取所有报告
    :return:
    """
    reports = Report.query.all()
    result = []
    for i in reports:
        result.append({"id": i.id, "userId": i.user_id, "title": i.report_title, "content": i.report_content})
    return jsonify(result)


@app.route('/report/getOne', methods=['GET'])
def report_get_one():
    """
    获取一个报告
    :return:
    """
    report_id = int(request.args.get('reportId'))
    report = db.session.query(Report).filter(Report.id == report_id).first()

    return {"id": report.id, "userId": report.user_id, "title": report.report_title, "content": report.report_content}


@app.route('/report/getReportByUserId', methods=['GET', 'POST'])
def report_get_by_user_id():
    """
    获取某个用户的所有报告
    :return:
    """
    user_id = request.args.get('userId')
    reports = db.session.query(Report).filter(Report.user_id == user_id)
    result = []
    for i in reports:
        result.append({"id": i.id, "userId": i.user_id, "title": i.report_title, "content": i.report_content})
    return jsonify(result)


@app.route('/report/deleteOne', methods=['POST'])
def report_delete_one():
    """
    删除一个报告
    :return:
    """
    try:
        report_id = int(request.form.get('reportId'))
        db.session.query(Report).filter(Report.id == report_id).delete()
        db.session.commit()
        return {"status": True, "message": "成功"}
    except:
        return {"status": False, "message": "失败"}


@app.route('/report/updateOne', methods=['POST'])
def report_update_one():
    """
    更新一个报告 title、content 传值为""是不更新
    :return:
    """
    try:
        report_id = request.form.get('reportId')
        report_title = request.form.get('title')
        report_content = request.form.get('content')

        update_content = {}
        if (report_title is not None) and (report_title != ''):
            update_content[Report.report_title] = report_title
        if (report_content is not None) and (report_content != ''):
            update_content[Report.report_content] = report_content

        db.session.query(Report).filter(Report.id == report_id).update(update_content)
        db.session.commit()
        return {"status": True, "message": "成功"}
    except:
        traceback.print_exc()
        return {"status": False, "message": "失败"}
