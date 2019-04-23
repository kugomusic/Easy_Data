from pyspark import SparkContext
sc = SparkContext("local","testing")
from  pyspark.mllib.fpm import FPGrowth
data = [["A", "B", "C", "E", "F","O"], ["A", "C", "G"], ["E","I"], ["A", "C","D","E","G"], ["A", "C", "E","G","L"],
       ["E","J"],["A","B","C","E","F","P"],["A","C","D"],["A","C","E","G","M"],["A","C","E","G","N"]]
# 转换成RDD 参数numSlices指定了将数据集切分为几份，这里不设置，Spark会尝试根据集群的状况，来自动设定slices的数目
rdd = sc.parallelize(data)
#支持度阈值为20%
model = FPGrowth.train(rdd, 0.3, 2)
print(sorted(model.freqItemsets().collect()))


from  pyspark.mllib.fpm import PrefixSpan
data = [
   [['a'],["a", "b", "c"], ["a","c"],["d"],["c", "f"]],
   [["a","d"], ["c"],["b", "c"], ["a", "e"]],
   [["e", "f"], ["a", "b"], ["d","f"],["c"],["b"]],
   [["e"], ["g"],["a", "f"],["c"],["b"],["c"]]
   ]
rdd = sc.parallelize(data)
model = PrefixSpan.train(rdd, 0.5,4)
print(sorted(model.freqItemsets().collect()))
