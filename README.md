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

Also add the input and output columns as stated in the TODO comments in the file.

The program will spit out an RMSE value in the console along with a bunch of warnings that can be ignored.
