from pyspark.sql import SparkSession
import pyspark.rdd

APP_NAME = "Random Forest Example2"
SPARK_URL = "spark://10.108.211.130:7077"
spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_URL) \
        .getOrCreate()
# data = spark.sparkContext.parallelize([('Ferrari','fast'),{'Porsche':10000},['Spain','visited',4504]]).collect()
# arr = []
# arr.append(data)
# print(data[1]['Porsche'])
# print(data[2][1])
#
# df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
# df = spark.createDataFrame([(1, "John Doe", 22)], ("id", "name", "age"))
# df.show()
# arr2 = []
# arr2.append(df)
# for d in arr2:
#     d.show()
data = spark.sparkContext.textFile("hdfs://10.108.211.130/user/yufeng/files/spam.txt")
data2 = data.map(lambda x: len(x))
sum = data2.fold(0, (lambda x, y: x + y))

sum
