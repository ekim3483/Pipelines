'''
This script sets up a machine learning pipeline using Apache Spark, including data loading, feature assembly, scaling, and model training.

If you don't have Java already installed then copy and paste the below commands into your terminal before running the Python file:

sudo add-apt-repository ppa:webupd8team/java
sudo apt-get update
sudo apt-get install oracle-java8-installer
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

Run these commands to make sure you have Spark installed:
pip install pyspark==3.1.2
pip install findspark

And pandas:
pip install pandas

Also add the input and output columns as stated in the TODO comments. 

The program will output an RMSE value in your console, following a bunch of warnings which can be ignored.

To delete Parquet directories, run the following commands in terminal (https://stackoverflow.com/questions/37617263/how-to-delete-a-parquet-file-on-spark):

import shutil
shutil.rmtree('/folder_name')

'''


def warn(*args, **kwargs):
    pass
import warnings
warnings.warn = warn
warnings.filterwarnings('ignore')

import findspark
findspark.init()

from pyspark.ml.regression import LinearRegression
from pyspark.ml.classification import LogisticRegression

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.feature import StringIndexer

from pyspark.sql import SparkSession

from pyspark.ml import Pipeline

from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

spark = SparkSession.builder.appName("Machine Learning Pipeline using Spark").getOrCreate()

#TODO: add URL of csv file after "wget" below
wget 

# TODO: replace "____" with name of csv file
dataset = spark.read.csv("___.csv", header=True, inferSchema=True)

# Stage 1 - assemble the input columns into a single vector 
#TODO: replace "____" with column names
vectorAssembler = VectorAssembler(inputCols=["___", "___", "___"], outputCol="features")
# Stage 2 - scale the features using standard scaler
scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures")
# Stage 3 - create a linear regression instance
#TODO: replace "____" with column to predict
lr = LinearRegression(featuresCol="scaledFeatures", labelCol="___")

#Training and building the pipeline
pipeline = Pipeline(stages=[vectorAssembler, scaler, lr])
(trainingData, testData) = dataset.randomSplit([0.7, 0.3], seed=42)
model = pipeline.fit(trainingData)

predictions = model.transform(testData)

#TODO: replace "___" with name of column to predict
evaluator = RegressionEvaluator(labelCol="___", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) =", rmse)

spark.stop()
