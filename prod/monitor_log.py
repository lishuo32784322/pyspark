# coding:utf-8
# author:ls
import time, datetime, json, re, os, sys, random, shutil
import requests as r


def main():
    check_time = int(time.time())
    mtime = int(os.stat('/mnt/bigdata_workspace/pyspark/prod/offset.txt').st_mtime)
    time_d = check_time - mtime
    print(time_d)
    if time_d > 590:
        url = 'http://10.0.1.30/tools/mqapi/add?impression_id=bd_app_log&topic=prod_alarm&key=app_log&value={"sub_sys_id" : 11, "alarm_grade": "error", "alarm_code" : 111001, "alarm_title" : "日志服务报警","alarm_msg" : "拉取kafka日志错误","alarm_time": %s}'%int(time.time())
        res = r.get(url=url) 
main()


