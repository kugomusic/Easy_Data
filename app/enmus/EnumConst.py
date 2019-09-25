# 导入枚举类
from enum import Enum


# 继承枚举类
# 算子编码枚举类
class operatorType(Enum):
    ## 数据预处理
    # 过滤
    FILTER = '1001'
    # 排序
    SORT = '1002'
    # 按列拆分
    COLUMNSPLIT = '1003'
    # 按行拆分
    ROWSPLIT = '1004'
    # 多列合并
    COLUMNMERGE = '1005'
    # 数据列替换
    REPLACE = '1006'
    # 空值填充
    FILLNULLVALUE = '1007'
    # 列映射
    COLUMNMAP = '1008'

    ## 特征工程
    # 分位数离散化
    QUANTILEDISCRETIZATION = '2001'
    # 向量索引转换
    VECTORINDEXER = '2002'
    # 标准化列
    STANDARDSCALER = '2003'
    # 降维
    PCA = '2004'
    # 字符串转标签
    STRINGINDEXER = '2005'
    # 独热编码
    ONEHOTENCODER= '2006'
    # 多项式扩展
    POLYNOMIALEXPANSION= '2007'
    # 卡放选择
    CHISQSELECTOR= '2008'
