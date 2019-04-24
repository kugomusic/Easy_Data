"""
Random Forest Classification Example.
"""
from pyspark import SparkContext
from pyspark.sql import SparkSession

if __name__ == "__main__":

    CSV_PATH = "/home/zk/data/creditcard.csv"
    APP_NAME = "Random Forest Example"
    SPARK_URL = "local[*]"
    RANDOM_SEED = 13579
    TRAINING_DATA_RATIO = 0.7
    RF_NUM_TREES = 3
    RF_MAX_DEPTH = 4
    RF_MAX_BINS = 32

    spark = SparkSession.builder \
        .appName(APP_NAME) \
        .master(SPARK_URL) \
        .getOrCreate()

    df = spark.read \
        .options(header="true", inferschema="true") \
        .csv(CSV_PATH)

    print("Total number of rows: %d" % df.count())

    from pyspark.mllib.linalg import Vectors
    from pyspark.mllib.regression import LabeledPoint

    transformed_df = df.rdd.map(lambda row: LabeledPoint(row[-1], Vectors.dense(row[0:-1])))

    splits = [TRAINING_DATA_RATIO, 1.0 - TRAINING_DATA_RATIO]
    training_data, test_data = transformed_df.randomSplit(splits, RANDOM_SEED)

    print("Number of training set rows: %d" % training_data.count())
    print("Number of test set rows: %d" % test_data.count())

    from pyspark.mllib.tree import RandomForest
    from time import *
    import shutil, os

    start_time = time()

    model = RandomForest.trainClassifier(training_data, numClasses=2, categoricalFeaturesInfo={}, \
                                         numTrees=RF_NUM_TREES, featureSubsetStrategy="auto", impurity="gini", \
                                         maxDepth=RF_MAX_DEPTH, maxBins=RF_MAX_BINS, seed=RANDOM_SEED)
    if os.path.exists("myRandomForestClassificationModel"):
        shutil.rmtree("myRandomForestClassificationModel")
    model.save(spark.sparkContext, "myRandomForestClassificationModel")

    print('Learned classification forest model:')
    print(model.numTrees())
    print(model.totalNumNodes())
    print(model.toDebugString())
    end_time = time()
    elapsed_time = end_time - start_time
    print("Time to train model: %.3f seconds" % elapsed_time)

    predictions = model.predict(test_data.map(lambda x: x.features))
    labels_and_predictions = test_data.map(lambda x: x.label).zip(predictions)
    acc = labels_and_predictions.filter(lambda x: x[0] == x[1]).count() / float(test_data.count())
    print("Model accuracy: %.3f%%" % (acc * 100))

    from pyspark.mllib.evaluation import BinaryClassificationMetrics

    start_time = time()

    metrics = BinaryClassificationMetrics(labels_and_predictions)
    print("Area under Precision/Recall (PR) curve: %.f" % (metrics.areaUnderPR * 100))
    print("Area under Receiver Operating Characteristic (ROC) curve: %.3f" % (metrics.areaUnderROC * 100))

    end_time = time()
    elapsed_time = end_time - start_time
    print("Time to evaluate model: %.3f seconds" % elapsed_time)
