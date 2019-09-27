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
dt = str(time.strftime("%Y%m%d", time.localtime(time.time()-3600*24)))


def create_table(data, name):
    table =[]
    for index,i in enumerate(data):
        index += 1
        table.append(f'<tr><td>{index}</td><td>{i.title}</td><td>{i.description}</td><td>{i.tag}</td><td>{i.p_tag}</td><td>{i.time}</td><td>{i.post_id}</td></tr>')
    text = f'''
            <table border="1" width="100%" bgcolor="#e9faff">
               <th colspan="7" align="center">{dt}{name}统计表</td>
               <tr align="center">
                   <th>排名</th>
                   <th>贴子标题</th>
                   <th>贴子描述</th>
                   <th>tag</th>
                   <th>标签</th>
                   <th>{name}</th>
                   <th>贴子ID</th>
               </tr>
               {''.join(table)}
           </table>
            '''
    return text
    


def count_top_view(hive_context):
    view = hive_context.sql(f'select event from prod_bd_app_log.dwv_bd_app_log where dt={dt} and event["event_id"]="view" and event["source"] in (1,3,4)').selectExpr('event.source','event.post_id','event.end_time-event.start_time as time')
    datas = view.join(hive_context.sql('select a.post_id,a.title,a.description,c.name as tag,d.name as p_tag from (select post_id,title,description from prod_bd_mysql_syn.dim_post_info) a left join (select post_id,tag_id from prod_bd_mysql_syn.dim_post_tag) b on a.post_id=b.post_id left join (select id,name,p_id from prod_bd_mysql_syn.dim_tag_info) c on b.tag_id=c.id left join (select id,name from prod_bd_mysql_syn.dim_tag_info) d on c.p_id=d.id'),['post_id'],'left').registerTempTable('view_join_data')
    datas = hive_context.sql('select d.post_id,substring(first(d.title),1,20) as title,substring(first(description),1,50) as description,ROUND(sum(d.time),2) as time,concat_ws(",",collect_set(tag)) as tag, first(d.p_tag) as p_tag from view_join_data d group by d.post_id order by time desc limit 100')
    return datas.take(100)

def count_top_play(hive_context):
    play = hive_context.sql(f'select event from prod_bd_app_log.dwv_bd_app_log where dt={dt} and event["event_id"]="play" and event["source"] in (3,4)').selectExpr('event.source','event.post_id','event.play_duration/1000 as time')
    datas = play.join(hive_context.sql('select a.post_id,a.title,a.description,c.name as tag,d.name as p_tag from (select post_id,title,description from prod_bd_mysql_syn.dim_post_info) a left join (select post_id,tag_id from prod_bd_mysql_syn.dim_post_tag) b on a.post_id=b.post_id left join (select id,name,p_id from prod_bd_mysql_syn.dim_tag_info) c on b.tag_id=c.id left join (select id,name from prod_bd_mysql_syn.dim_tag_info) d on c.p_id=d.id'),['post_id'],'left').registerTempTable('play_join_data')
    datas = hive_context.sql('select d.post_id,substring(first(d.title),1,20) as title, substring(first(description),1,50) as description, ROUND(sum(d.time),2) as time,concat_ws(",",collect_set(tag)) as tag, first(d.p_tag) as p_tag from play_join_data d group by d.post_id order by time desc limit 100')
    return datas.take(100)

def count_top_ctr(hive_context):
    df = hive_context.sql(f'select event from prod_bd_app_log.dwv_bd_app_log where dt={dt}')
    view = df.filter("event.event_id='view'").groupBy('event.post_id').agg({'event.event_id':'count'}).withColumnRenamed('count(event[event_id] AS `event_id`)','view_count')
    view_data = view.select('post_id','view_count').filter('view_count>1')
    play = df.filter("event.event_id='play'").groupBy('event.post_id').agg({'event.event_id':'count','event.play_duration':'sum'}).withColumnRenamed('count(event[event_id] AS `event_id`)','play_count')
    play_data = play.select('post_id','play_count')
    join_data = play_data.join(view_data,['post_id'],'left')
    join_data = join_data.selectExpr('post_id','play_count/view_count as ctr')
    datas = join_data.join(hive_context.sql('select a.post_id,a.title,a.description,c.name as tag,d.name as p_tag from (select post_id,title,description from prod_bd_mysql_syn.dim_post_info) a left join (select post_id,tag_id from prod_bd_mysql_syn.dim_post_tag) b on a.post_id=b.post_id left join (select id,name,p_id from prod_bd_mysql_syn.dim_tag_info) c on b.tag_id=c.id left join (select id,name from prod_bd_mysql_syn.dim_tag_info) d on c.p_id=d.id'),['post_id'],'left').registerTempTable('ctr_join_data')
    datas = hive_context.sql('select d.post_id,substring(first(d.title),1,20) as title, substring(first(description),1,50) as description, max(ctr) as time,concat_ws(",",collect_set(tag)) as tag, first(d.p_tag) as p_tag from ctr_join_data d group by d.post_id order by time desc limit 100')
    return datas.take(100)

def main():
    conf = SparkConf().setMaster("yarn-client").setAppName('Top100').set('spark.executorEnv.PYTHONHASHSEED','0')
    spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    hive_context = HiveContext(sc)
    view = create_table(count_top_view(hive_context),'曝光时长')
    play = create_table(count_top_play(hive_context),'播放时长')
    ctr  = create_table(count_top_ctr (hive_context),'CTR')
    with open(f'/mnt/bigdata_workspace/pyspark/prod/bd_send_email/top_html/每日曝光时长统计报表.html','w') as f:
        f.write(view)
    with open(f'/mnt/bigdata_workspace/pyspark/prod/bd_send_email/top_html/每日播放时长统计报表.html','w') as f:
        f.write(play)
    with open(f'/mnt/bigdata_workspace/pyspark/prod/bd_send_email/top_html/每日CTR统计报表.html','w') as f:
        f.write(ctr)
    
main()
