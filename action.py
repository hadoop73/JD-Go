# coding:utf-8


from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql import functions
from pyspark.sql.functions import udf
import datetime
import os

os.environ["SPARK_HOME"] = "/home/hadoop/spark-2.0.1-bin-hadoop2.7"   #KeyError: 'SPARK_HOME'
conf = SparkConf()
conf.setMaster('spark://192.168.1.116:7077')
sc=SparkContext(appName='Action',conf=conf)
sqlContext = SQLContext(sc)

#df = sqlContext.read.format('com.databricks.spark.csv').options(header='true',charset = "GBK").load('hdfs://192.168.1.116:9000/home/JData/JData_User.csv')
df = sqlContext.read.format('com.databricks.spark.csv').options(header='true',charset = "GBK").load('hdfs://192.168.1.116:9000/home/JData/JData_Action_201604.csv')

# 字符串转化为 datetime
df = df.withColumn("time", df["time"].cast(TimestampType()))

# 筛选数据
now = datetime.datetime(2016,4,9,0,0,0)
df = df.filter(df.time>=now)

# 数据去重
df = df.dropDuplicates(['user_id', 'sku_id','time','type'])

# 统计每个 type 的次数
df = df.withColumn('count', functions.lit(1))
# 统计每个用户对 type 的次数
df = df.groupby(['user_id','sku_id','type']).agg({'count':'sum'})
df = df.withColumnRenamed('sum(count)','typeN')



for i in range(1,7):
    def func(x,y):
        if int(x)==i:
            return float(y)
        else:
            return float(0)

    f = udf(func ,DoubleType())
    df = df.withColumn("type#"+str(i),f('type','typeN'))
    print df.show(5)

df.toPandas().to_csv('act.csv')  # 能够保存到本地
#df.write.csv('action.csv')

