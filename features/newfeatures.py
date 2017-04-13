# coding:utf-8
from pyspark import SparkContext,SparkConf
from pyspark.sql import SQLContext
from pyspark.sql.catalog import Column
from pyspark.sql.types import *
from pyspark.sql import functions
from pyspark.sql.functions import udf
from datetime import datetime,timedelta
import os
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import UserDefinedFunction,rank, col
from pyspark.sql.types import LongType,StringType

os.environ["SPARK_HOME"] = "/home/hadoop/spark-2.0.1-bin-hadoop2.7"   #KeyError: 'SPARK_HOME'
conf = SparkConf()
conf.set("spark.hadoop.validateOutputSpecs", "false")
conf.setMaster('spark://master:7077')
sc=SparkContext(appName='p1-sample',conf=conf)
sqlContext = SQLContext(sc)

df = sqlContext.read.format('com.databricks.spark.csv').options(header='true', charset="utf-8").load(
    'hdfs://192.168.1.116:9000/home/hadoop/data2/JData34-Sample3.csv')

def getLabel(t):
    if int(t)==4:
        return 1
    return 0

# 4 组购买数据构成正样本数据，并进行特征提取
def getPast35DasyNumsPPdata(n=3):
        day = timedelta(days=1)
        dend = datetime(2016, 4, 16)
        data = None
        try:
            data = sqlContext.read.format('com.databricks.spark.csv')\
                .options(header='true', charset="utf-8").load(
                'hdfs://192.168.1.116:9000/home/hadoop/data2/JData34_p.csv')
        except:
            for i in range(4):
                dmid = dend - 4 * i * day
                mtimestr = datetime.strftime(dmid, '%Y-%m-%d')
                dstart = dmid - 4 * day
                stimestr = datetime.strftime(dstart, '%Y-%m-%d')

                con = "'{}' < time and time < '{}'".format(stimestr, mtimestr)
                print con
                df3 = df.filter(con).coalesce(10).select(['user_id','type'])
                # 把 type=4 生成 label=1
                getLabelUdf = UserDefinedFunction(getLabel,IntegerType())
                # 获得 user_id label 数据
                df3 = df3.withColumn('label', getLabelUdf(df3['type']))\
                    .groupBy('user_id').max('label')\
                    .withColumnRenamed('max(label)','label')
                    #.filter('label=1')
                df3.persist(StorageLevel.MEMORY_AND_DISK)

                dlast = dstart - 35 * day
                ltimestr = datetime.strftime(dlast, '%Y-%m-%d')
                con = "'{}' < time and time < '{}'".format(ltimestr, stimestr)
                # 获取过去 35 天的所有不同用户用来选取正负样本，在这里，只选取一个负样本，因为负样本太多了 4w
                # 而正样本只有 4k ，所以进行了负采样相似的操作，补全正样本
                df35 = df.filter(con).coalesce(10).select('user_id').dropDuplicates(['user_id'])
                df35.persist(StorageLevel.MEMORY_AND_DISK)
                """
                try:
                    dn = sqlContext.read.format('com.databricks.spark.csv').options(header='true',
                         charset="utf-8").load(
                        'hdfs://192.168.1.116:9000/home/hadoop/data2/JData34_n.csv')
                except:
                    if not data:
                        # 处理负样本
                        df350 = df35.join(df3.filter('label=0').coalesce(10), 'user_id')
                        dn = getFeatures(df350,dstart)
                        del df350
                        print dn.show(5)
                        print "-"*40
                        dn.write.csv("/home/hadoop/data2/JData34_n.csv",header=True,mode="overwrite")
                """
                # 正样本，大概 2k - 4k 左右
                df351 = df35.join(df3.filter('label=1'),'user_id')
                if not data:
                    data = df351
                else:
                    data = data.union(df351)
                data.persist(StorageLevel.MEMORY_AND_DISK)
            print data.show(5)
            print "-" * 40
            data.write.csv("/home/hadoop/data2/JData34_p.csv",header=True,mode="overwrite")
            #getPfeature(df31,dstart,dmid)


# 4 组购买数据构成正样本数据，并进行特征提取
def getPast35DasyNumsNNdata(n=3):
        day = timedelta(days=1)
        dend = datetime(2016, 4, 16)
        data = None
        try:
            data = sqlContext.read.format('com.databricks.spark.csv')\
                .options(header='true', charset="utf-8").load(
                'hdfs://192.168.1.116:9000/home/hadoop/data2/JData34_n.csv')
        except:
                dmid = dend - 4 * day
                mtimestr = datetime.strftime(dmid, '%Y-%m-%d')
                dstart = dmid - 4 * day
                stimestr = datetime.strftime(dstart, '%Y-%m-%d')
                # 筛选 4 中的用户，从这些用户中来选择正负样本
                # 正负样本必须与过去 35 天有交集，只有这样才能利用历史记录进行预测
                con = "'{}' < time and time < '{}'".format(stimestr, mtimestr)
                df3 = df.filter(con).coalesce(10).select(['user_id','type'])
                # 把 type=4 生成 label=1
                getLabelUdf = UserDefinedFunction(getLabel,IntegerType())
                # 获得 user_id label 数据
                df3 = df3.withColumn('label', getLabelUdf(df3['type']))\
                    .groupBy('user_id').max('label')\
                    .withColumnRenamed('max(label)','label')
                    #.filter('label=1')
                df3.persist(StorageLevel.MEMORY_AND_DISK)

                dlast = dstart - 35 * day
                ltimestr = datetime.strftime(dlast, '%Y-%m-%d')
                con = "'{}' < time and time < '{}'".format(ltimestr, stimestr)
                # 获取过去 35 天的所有不同用户用来选取正负样本，在这里，只选取一个负样本，因为负样本太多了 4w
                # 而正样本只有 4k ，所以进行了负采样相似的操作，补全正样本
                df35 = df.filter(con).coalesce(10).select('user_id').dropDuplicates(['user_id'])
                df35.persist(StorageLevel.MEMORY_AND_DISK)

                # 负样本，大概 4w 左右
                df350 = df35.join(df3.filter('label=0'),'user_id')

                print df350.show(5)
                print df350.count()
                print "-" * 40
                #df350.write.csv("/home/hadoop/data2/JData34_n.csv",header=True,mode="overwrite")


def genFeatures(d=df,start="2016-04-12",path=""):
    if not d or len(start)<1:
        raise ValueError("输入参数错误！！")
    """
    1. 统计 sku,cate 前3特征
    2. 统计用户action的时间间隔
    3. 统计用户action距离start的时间间隔
    4. 首先把时间变成天
    5. 统计一天中的action次数，最多次数
    """
    # 时间按天处理
    time10 = UserDefinedFunction(lambda x:x[:10],StringType)
    d = d.withColumn('time',time10(d['time']))
    d.persist()
    dtime = d.groupby('user_id').agg({'time':"max","time":"min"})
    tmp = UserDefinedFunction(lambda x:str())
    dtime = dtime.withColumn('diff-start')

getPast35DasyNumsNNdata()






