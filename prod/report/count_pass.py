#coding=utf-8
import os,time
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext
from pyspark.sql.window import Window
from pyspark.sql import Row, functions as F


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"

# ip:port或者域名
uri = "mongodb://root:BDqilingzhengfan1@10.0.1.19:27017"
# database name
database = "all_post"
# collection name
collection = "post_data"

def main():
    conf = (SparkConf().setMaster("yarn-client").setAppName('').set('spark.executorEnv.PYTHONHASHSEED','0'))
    conf.set("spark.mongodb.input.uri", uri)
    conf.set("spark.mongodb.input.database", database)
    conf.set("spark.mongodb.input.collection", collection)
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    sc.setLogLevel(logLevel='ERROR')
    hive_context = HiveContext(sc)
    origin_data = hive_context.read.format("com.mongodb.spark.sql").options(uri=uri, database=database, collection=collection).option("pipeline",'[{"$project":{"origin_user_id":1,"origin_post_id":1,"_id":0}}]').load()
    map_post_id = hive_context.sql('select post_id,origin_post_id as map_origin_post_id,user_id from prod_bd_mysql_syn.dim_post_upload where post_state = 1')
    post_id_status = hive_context.sql('select post_id as map_post_id,rec_status from prod_bd_mysql_syn.dim_post_info')
    map_post = origin_data.join(map_post_id,origin_data.origin_post_id==map_post_id.map_origin_post_id,'left_outer')
    map_data = map_post.join(post_id_status,map_post.post_id==post_id_status.map_post_id,'left_outer').select('origin_user_id','origin_post_id','post_id','user_id','rec_status').filter('rec_status is null')
    print(map_data.take(500),map_data.count())




main()
