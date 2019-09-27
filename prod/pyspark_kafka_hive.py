import os
import time
import json
from pyspark.sql import SparkSession
from pyspark import SparkConf
from pyspark.streaming import StreamingContext
from pyspark.sql import HiveContext
from pyspark.streaming.kafka import KafkaUtils, TopicAndPartition
import logging
logging.basicConfig(level=logging.INFO, filename=str(os.path.join(os.path.dirname(os.path.realpath(__file__)), "log/logger.log")), format='%(asctime)s - %(levelname)s: %(message)s')
logger = logging.getLogger(__name__)


os.environ["PYSPARK_PYTHON"] = "/usr/bin/python3"
os.environ["PYSPARK_DRIVER_PYTHON"] = "/usr/bin/python3"


conf = SparkConf().setMaster("yarn-client").setAppName('pyspark_read_kafka_prod').set('spark.executorEnv.PYTHONHASHSEED','0')
spark = SparkSession.builder.config(conf=conf).enableHiveSupport().getOrCreate()
sc = spark.sparkContext
ssc = StreamingContext(sparkContext=sc, batchDuration=600)
hive_context = HiveContext(sc)
topic = 'prod_bd_log_report'
brokers = "10.0.4.5:6667,10.0.4.2:6667,10.0.4.4:6667"


def storeOffsetRanges(rdd):
    offsetRanges = rdd.offsetRanges()
    for i in offsetRanges:
        if i.partition == 0:
            start_0 = i.fromOffset
        if i.partition == 1:
            start_1 = i.fromOffset
        if i.partition == 2:
            start_2 = i.fromOffset
    logger.warning('{"start_0":%s,"start_1":%s,"start_2":%s}'%(start_0,start_1,start_2))
    print('{"start_0":%s,"start_1":%s,"start_2":%s}'%(start_0,start_1,start_2))
    record_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "offset.txt")
    with open(record_path, 'w') as f:
        f.write('{"start_0":%s,"start_1":%s,"start_2":%s}'%(start_0,start_1,start_2))
    return rdd



def format_data(rdd):
    info = rdd[1].replace('data=', '').replace('\n','').replace(' ','').split('&sign',maxsplit=1)
    if info:
        data = json.loads(str(info[0]))
        if 'time' not in data.keys() and 'sign_t' in data.keys():
            data['time'] = data['sign_t']
        if 'app_name' not in data.keys():
            data['app_name'] = '百搭err'
        if 'ab_id' not in data.keys():
            data['ab_id'] = 'ab_err'
        if data:
            real_ip = 'sign'+info[1]
            ip = [i.replace('real_ip=','') for i in real_ip.split('&') if 'real_ip' in i]
            if ip:
                data['ip'] = ip[0]
    return data

def get_event(data):
    data_list = []
    for i in data['event']:
        try:
            key_list = []
            for key in i.keys():
                key_list.append(key)
            if 'impression_id' not in key_list and 'impression_Id' in key_list:
                i['impression_id'] = i['impression_Id']
                i.pop('impression_Id')
                key_list.append('impression_id')
                key_list.remove('impression_Id')
            if 'impression_id' not in key_list and 'impressionId' in key_list:
                i['impression_id'] = i['impressionId']
                i.pop('impressionId')
                key_list.append('impression_id')
                key_list.remove('impressionId')
            keys = ','.join(['%s:%s'%(str(key), i[key]) for key in key_list])
            data_list.append(f"{str(data['app_name'])}\t{str(data['os'])}\t{str(data['v_name'])}\t{str(data['pc_id'])}\t{str(data['v_app'])}\t{str(data['ip'])}\t{str(data['ab_id'])}\t{str(data['user_id'])}\t{str(data['dev_id'])}\t{str(data['udid'])}\t{str(data['device_token'])}\t{str(data['v_os'])}\t{str(data['model'])}\t{str(data['brand'])}\t{str(data['facturer'])}\t{str(data['resolution'])}\t{str(data['net'])}\t{str(data['carrier'])}\t{str(data['time'])}\t{keys}")
        except:pass
    data_list = '\n'.join(data_list)
    return data_list


def filter_event(rdd):
    for i in rdd['event']:
        if i['event_id'] == '' or i['event_id'] is None or len(str(i['event_id']))<2:
            rdd['event'].remove(i)
    return rdd


def process(rdd):
    filename = str(time.strftime("%Y%m%d_%H-%M", time.localtime(time.time())))
    partition_date = str(time.strftime("%Y%m%d", time.localtime(time.time()-600)))
    if rdd:
        data = rdd.map(get_event)
        data.saveAsTextFile(f'hdfs:///user/bigdata/prod/tmp_app_log/{filename}')
        filter_data = rdd.map(filter_event).filter(lambda x: x['dev_id'] != '' and len(str(x['dev_id'])) > 2 and x['event'] is not None and x['dev_id'] is not None).map(get_event)
        filter_data.saveAsTextFile(f'hdfs:///user/bigdata/prod/tmp_app_log/filter_{filename}')
        hive_context.sql(f"load data inpath '/user/bigdata/prod/tmp_app_log/{filename}' into table prod_bd_app_log.src_bd_app_log partition (dt={partition_date})")
        hive_context.sql(f"load data inpath '/user/bigdata/prod/tmp_app_log/filter_{filename}' into table prod_bd_app_log.dwv_bd_app_log partition (dt={partition_date})")
    

def f(rdd):
    info = rdd[1].replace('data=', '').replace('\n','').replace(' ','').split('&')
    if info:
        data = json.loads(str(info[0]))
        key_list = []
        for key in data.keys():
            key_list.append(str(key))
        return key_list


def main():
    record_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "offset.txt")
    with open(record_path, 'r') as f:
        start = json.loads(f.read())
    start_0, start_1, start_2 = start['start_0'], start['start_1'], start['start_2']
    
    kafkaStreams = KafkaUtils.createDirectStream(ssc, [topic], kafkaParams={"metadata.broker.list": brokers},
                                                 fromOffsets={TopicAndPartition(topic, 0): int(start_0),
                                                              TopicAndPartition(topic, 1): int(start_1),
                                                              TopicAndPartition(topic, 2): int(start_2)})
    kafkaStreams.transform(storeOffsetRanges).map(format_data).foreachRDD(process)

    ssc.start()
    ssc.awaitTermination()


if __name__ == '__main__':
    main()
