# -*- coding: UTF-8 -*-
from app.models.mysql import DataSource, Project,initdb
import os
# 获取项目的正在操作的数据文件地址
def getProjectCurrentDataUrl(projectName):
    try:
        filters = {
            Project.project_name == projectName
        }
        Pro = Project.query.filter(*filters).first()
        ProjectAddress = Pro.project_address
        # print(ProjectAddress)
        filename = ''
        for root, dirs, files in os.walk(ProjectAddress):
            # print(root) #当前目录路径
            # print(dirs) #当前路径下所有子目录
            # print(files) #当前路径下所有非目录子文件
            for file in files:
                if file[-4:] =='.csv':
                    filename = file
                    break
            break
        # print(filename)
        if filename == '':
            return "error"
        else:
            return {'fileUrl': ProjectAddress+'/'+filename, 'projectAddress': ProjectAddress}
    except:
        return "error"

#根据指定路径创建文件夹
def mkdir(path):
    # 引入模块
    import os

    # 去除首位空格
    path=path.strip()
    # 去除尾部 \ 符号
    path=path.rstrip("\\")

    # 判断路径是否存在
    # 存在     True
    # 不存在   False
    isExists=os.path.exists(path)

    # 判断结果
    if not isExists:
        # 如果不存在则创建目录
        print(path+' 创建成功')
        # 创建目录操作函数
        os.makedirs(path)
        return True
    else:
        # 如果目录存在则不创建，并提示目录已存在
        print(path+' 目录已存在')
        return False
