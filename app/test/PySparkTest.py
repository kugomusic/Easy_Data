from pyspark.sql import SparkSession

APP_NAME = "Random Forest Example"
SPARK_URL = "local[*]"
spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_URL) \
        .getOrCreate()
data = spark.sparkContext.parallelize([('Ferrari','fast'),{'Porsche':10000},['Spain','visited',4504]]).collect()
print(data[1]['Porsche'])
print(data[2][1])
