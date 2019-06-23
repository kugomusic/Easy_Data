# -*- coding: UTF-8 -*-
from app.utils import getProjectCurrentDataUrl
import pandas as pd

def fullTableStatistics2():
    # columnNames = request.form.getlist("columns")
    # projectName = request.form.getlist("projectName")
    columnNames = [ "行 ID",
                    "订单 ID",
                    "订购日期",
                    "装运日期",
                    "装运方式",
                    "客户 ID",
                    "客户名称",
                    "细分市场",
                    "邮政编码 (Postal Code)",
                    "城市 (City)",
                    "省/市/自治区 (State/Province)",
                    "国家/地区 (Country)",
                    "地区",
                    "市场",
                    "产品 ID",
                    "类别",
                    "子类别",
                    "产品名称",
                    "销售额",
                    "数量",
                    "折扣",
                    "利润",
                    "装运成本",
                    "订单优先级"]
    projectName = "爱德信息分析项目"
    fileUrl = getProjectCurrentDataUrl(projectName)
    if fileUrl[-4:] == ".csv":
        df_excel = pd.read_csv(fileUrl, encoding="utf-8")
    else:
        df_excel = pd.read_excel(fileUrl, encoding="utf-8")
    res = []
    statistics = ['字段名','类型','总数','最小值','最小值位置','25%分位数','中位数','75%分位数','均值','最大值','最大值位置','平均绝对偏差','方差','标准差','偏度','峰度']
    for columnName in columnNames:
        info = {}.fromkeys(statistics)
        info['字段名'] = columnName
        info['类型'] = df_excel[columnName].dtype
        if info['类型'] == 'int64' or info['类型'] == 'float64':
            info['总数'] = df_excel[columnName].count()
            info['最小值'] = df_excel[columnName].min()
            info['最小值位置'] = df_excel[columnName].idxmin()
            info['25%分位数'] = df_excel[columnName].quantile(.25)
            info['中位数'] = df_excel[columnName].median()
            info['75%分位数'] = df_excel[columnName].quantile(.75)
            info['均值'] = df_excel[columnName].mean()
            info['最大值'] = df_excel[columnName].max()
            info['最大值位置'] = df_excel[columnName].idxmax()
            info['平均绝对偏差'] = df_excel[columnName].mad()
            info['方差'] = df_excel[columnName].var()
            info['标准差'] = df_excel[columnName].std()
            info['偏度'] = df_excel[columnName].skew()
            info['峰度'] = df_excel[columnName].kurt()
        else:
            info['类型'] = "text"
        res.append(info)
    # print(res)

# fullTableStatistics2()

str1 = "利润,+,100,+,数量,*,0.0001,newCol"
print(len(str1.split(';'))) #1
str1 = "利润,均值填充;数量,最大值填充"
print(len(str1.split(';'))) #2
str1 = "利润,均值填充;数量,最大值填充;"
print(len(str1.split(';'))) #3