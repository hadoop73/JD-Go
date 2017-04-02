# coding:utf-8


from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *

import os
os.environ["SPARK_HOME"] = "/home/hadoop/spark-2.0.1-bin-hadoop2.7"   #KeyError: 'SPARK_HOME'
conf = SparkConf()
conf.setMaster('spark://192.168.1.116:7077')
sc=SparkContext(appName='MyApp',conf=conf)
sqlContext = SQLContext(sc)

#df = sqlContext.read.format('com.databricks.spark.csv').options(header='true',charset = "GBK").load('hdfs://192.168.1.116:9000/home/JData/JData_User.csv')
dfaction = sqlContext.read.format('com.databricks.spark.csv').options(header='true',charset = "GBK").load('hdfs://192.168.1.116:9000/home/JData/JData_Action_201603.csv')

adf = dfaction.groupBy(['user_id','sku_id','type']).count()
adf.persist()
print adf.show(5)


