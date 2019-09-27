import pyhdfs
import time

fs = pyhdfs.HdfsClient('10.0.4.10','8020')

filename = '/user/bigdata/prod/tmp_app_log/'
today = int(time.strftime("%Y%m%d", time.localtime(time.time()-259200)))
for i in fs.listdir('/user/bigdata/prod/tmp_app_log'):
    print(i)
    for name in [i for i in i.split('_') if len(i) == 8]:
        print(int(name), today)
        if int(name) < today:
            fs.delete(filename+str(i),recursive=True)           



