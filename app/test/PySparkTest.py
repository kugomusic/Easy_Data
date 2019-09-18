from pyspark.sql import SparkSession

APP_NAME = "Random Forest Example"
SPARK_URL = "local[*]"
spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_URL) \
        .getOrCreate()
data = spark.sparkContext.parallelize([('Ferrari','fast'),{'Porsche':10000},['Spain','visited',4504]]).collect()
arr = []
arr.append(data)
print(data[1]['Porsche'])
print(data[2][1])

df = spark.createDataFrame([(1, "John Doe", 21)], ("id", "name", "age"))
df = spark.createDataFrame([(1, "John Doe", 22)], ("id", "name", "age"))
df.show()
arr2 = []
arr2.append(df)
for d in arr2:
    d.show()
