#coding=utf-8
import os,time
from datetime import datetime
from sqlalchemy import create_engine
import requests as r
import pandas as pd
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark.sql import HiveContext



os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"
engine = create_engine('mysql+pymysql://bd_ceshi:BDqilingzhengfan1@10.0.1.15:3306/bd_app_log')
dt = str(time.strftime("%Y%m%d", time.localtime(time.time()-3600*24)))

def join_key(rdd):
    try:
        return (str(rdd[8])+str(rdd[-2]['post_id']),rdd)
    except:
        url = 'http://10.0.1.30/tools/mqapi/add?impression_id=bd_app_log&topic=prod_alarm&key=app_log&value={"sub_sys_id" : 11, "alarm_grade": "error", "alarm_code" : 111001, "alarm_title" : "日志服务报警","alarm_msg" : "日志解析错误:%s","alarm_time": %s}'%(rdd, int(time.time()))
        res = r.get(url=url)

def join_key_play(rdd):
    try:
        return (str(rdd[8])+str(rdd[-2]['post_id']), int(rdd[-2]['duration']))
    except:
        url = 'http://10.0.1.30/tools/mqapi/add?impression_id=bd_app_log&topic=prod_alarm&key=app_log&value={"sub_sys_id" : 11, "alarm_grade": "error", "alarm_code" : 111001, "alarm_title" : "日志服务报警","alarm_msg" : "日志解析错误:%s","alarm_time": %s}'%(rdd, int(time.time()))

def count_dau(df):
    df = df.dropDuplicates(subset=['udid'])
    return int(df.count())

def count_click(df, dau):
    df = df.filter(df['event']['event_id']=='click')
    df = df.rdd.map(join_key).filter(lambda x:x).reduceByKey(lambda x,y:x)
    return int(df.count())//dau

def count_view(df, dau):
    df = df.filter(df['event']['event_id']=='view')
    df = df.rdd.map(join_key).filter(lambda x:x).reduceByKey(lambda x,y:x)
    return int(df.count())//dau

def count_play(df, dau):
    df = df.filter(df['event']['event_id']=='play')
    def join_key_play(rdd):
        try:
            return (str(rdd[8])+str(rdd[-2]['post_id']), int(rdd[-2]['play_duration']))
        except:
            url = 'http://10.0.1.30/tools/mqapi/add?impression_id=bd_app_log&topic=prod_alarm&key=app_log&value={"sub_sys_id" : 11, "alarm_grade": "error", "alarm_code" : 111001, "alarm_title" : "日志服务报警","alarm_msg" : "日志解析错误:%s","alarm_time": %s}'%(rdd, int(time.time()))
            res = r.get(url=url)
    df = df.rdd.map(join_key_play).filter(lambda x:x).reduceByKey(lambda x,y:x).map(lambda x:x[1]).reduce(lambda x,y:x+y)
    return int(df)//dau//1000

def main():
    conf = SparkConf().setMaster("yarn-client").setAppName('count').set('spark.executorEnv.PYTHONHASHSEED','0')
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    hive_context = HiveContext(sc)
    all_data_sql = "select * from prod_bd_app_log.dwv_bd_app_log where dt=%s"%dt
    df = hive_context.sql(all_data_sql)
    dau = count_dau(df)
    play = count_play(df, dau)

    df = df.filter(df['event']['source']==1)    
    click = count_click(df, dau)
    view = count_view(df, dau)
    week = datetime.strptime(dt, '%Y%m%d').weekday()+1
    to_sql = 'insert into report(dt,week,dau,click,view,play) values(%s,%s,%s,%s,%s,%s)'%(dt,week,dau,click,view,play)
    try:
        pd.read_sql(to_sql, con=engine)
    except Exception as e:
        print(e)
    

main()
