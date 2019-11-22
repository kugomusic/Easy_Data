class Const(object):
    class ConstError(TypeError):
        pass

    class ConstCaseError(ConstError):
        pass

    def __setattr__(self, name, value):
        if name in self.__dict__:  # 判断是否已经被赋值，如果是则报错
            raise self.ConstError("Can't change const.%s" % name)
        if not name.isupper():  # 判断所赋值是否是全部大写，用来做第一次赋值的格式判断，也可以根据需要改成其他判断条件
            raise self.ConstCaseError('const name "%s" is not all supercase' % name)

        self.__dict__[name] = value


const = Const()

const.ROOTURL = "/home/zk/project/"
# const.ROOTURL = "/Users/kang/PycharmProjects/project"

# csv文件存储目录（临时）
const.SAVEDIR = "/home/zk/project/test.csv"
# const.SAVEDIR = '/Users/kang/PycharmProjects/project/test.csv'
# const.SAVEDIR = "/Users/tc/Desktop/可视化4.0/Project/test.csv"

# exploration 临时视图存放
const.JSONFILENAME = 'qazwsxedcrfvtgbyhnujmiopkl' + '.json'

# 算子运行产生的中间数据
const.MIDDATA = '/home/zk/midData/'
