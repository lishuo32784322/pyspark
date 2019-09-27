from pyhive import hive
import time


conn = hive.Connection(host='10.0.4.10', port=10500, username='bigdata', database='prod_bd_app_log')
cursor = conn.cursor()

dt = str(time.strftime("%Y%m%d", time.localtime(time.time()-3600*24)))
src_sql = f"insert overwrite table prod_bd_app_log.src_bd_app_log partition(dt={dt}) select app_name,os,v_name,pc_id,v_app,ip,ab_id,user_id,dev_id,udid,device_token,v_os,model,brand,facturer,resolution,net,carrier,time,event from prod_bd_app_log.src_bd_app_log where dt={dt}"
dwv_sql = f"insert overwrite table prod_bd_app_log.dwv_bd_app_log partition(dt={dt}) select app_name,os,v_name,pc_id,v_app,ip,ab_id,user_id,dev_id,udid,device_token,v_os,model,brand,facturer,resolution,net,carrier,time,event from prod_bd_app_log.dwv_bd_app_log where dt={dt}"

print(src_sql)
print(dwv_sql)

cursor.execute(src_sql)
cursor.execute(dwv_sql)
