# 依赖库
# flask相关
from flask import flash, get_flashed_messages, redirect, render_template, request, session, url_for, jsonify, Response, abort
from flask import jsonify, Response, abort
from werkzeug.security import check_password_hash, generate_password_hash
from app import app
from app import db

# 其他基础库
import os
import math
import threading
import time
from datetime import datetime
from PIL import Image


'''
仅作为网页定向使用
'''

@app.route('/') # 首页
@app.route('/index')
def index():
    return render_template('index.html') # 返回首页


'''
登陆注册模块
'''

@app.route('/login', methods=['GET', 'POST']) # 登陆
def login():

    try: # 验证session
        uid = session['uid']
        return redirect('dashboard')
    except:
        pass
    
    if request.method == 'POST': # POST请求
        uid = request.form.get('uid')
        password = request.form.get('password')
        u_data = User.query.filter_by(uid=uid).first()
        try:
            rp = u_data.password
            if check_password_hash(rp, password): # check password
                session['uid'] = uid # cookie
                try:
                    url = session['url'] # 返回之前选择的页面
                    return redirect(url)
                except:
                    pass
                return redirect("dashboard") # 返回个人主页工作台
            else:
                flash('密码错误,若忘记了密码请电邮dheming@ruc.edu.cn')
                return render_template('login.html', flash=flash)
        except:
            flash('用户不存在')
            return render_template('login.html', flash=flash)
    else:
        return render_template('login.html')

@app.route('/registered', methods=['GET', 'POST']) # 注册
def registered():
    if request.method == 'POST':

        uid = request.form.get('uid')  
        try:
            if User.query.filter_by(uid=uid).first().uid: # 验证用户名是否唯一
                flash('该用户名存在')
                return redirect('registered')
        except:
            pass
        if len(uid) < 6 or not uid.isalnum(): # 验证用户名格式
            flash('请输入正确的用户名格式')
            return redirect('registered')
    
        studentid = request.form.get('studentid') # 验证学号
        try:
            if User.query.filter_by(student_id=studentid).first().uid: # 验证学号是否唯一
                flash('该学号已被注册,若您有疑问或需要申诉请电邮dheming@ruc.edu.cn')
                return redirect('registered')
        except:
            pass
        try:
            if int(studentid) < 1000000000 or int(studentid) > 9999999999: # 验证学号规范
                return redirect('registered')
        except:
            flash('请输入正确的学号')
            return redirect('registered')

        password = request.form.get('password')
        rpassword = request.form.get('rpassword')
        if password != rpassword:  # 验证密码
            flash('两次密码不一致')
            return redirect('registered')
        
        email = request.form.get('email') # 获取邮箱信息

        password = generate_password_hash(password)  # 密码加密

        rss = request.form.get('rss') # 获取rss订阅选择
        if not rss:
            rss = '0'

        user = User(
            uid=uid,
            password=password,
            email=email,
            student_id=studentid,
            rss=rss,
            register_login=datetime.now()
        )

        db.session.add(user)  # 向数据库添加用户信息
        db.session.commit()

        session['uid'] = uid # 注册后自动登陆并返回工作台
        return redirect('dashboard')
    else:
        return render_template('registered.html')

# 登出
@app.route('/logout')
def logout():
    del session['uid']
    session['url'] = 'dashboard' # 重置url
    return redirect('index')


'''
404
'''

@app.errorhandler(404) # 全局404处理
def page_404(er):
    return render_template('404.html')

