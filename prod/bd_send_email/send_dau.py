import pandas as pd
from sqlalchemy import create_engine
import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import os
import vthread


my_sender = '17612472355@baidaweb.cn'
my_pass = 'Lishuo4322'
#my_user = ['wangjinglei@baidaweb.cn','lishuo@baidaweb.cn']
my_user = ['wangjinglei@baidaweb.cn','17612472355@baidaweb.cn','wanggang@baidaweb.cn','lizhiqiao@baidaweb.cn','lemon@baidaweb.cn','Zichang@baidaweb.cn']
engine = create_engine('mysql+pymysql://bd_ceshi:BDqilingzhengfan1@10.0.1.15:3306/bd_app_log')


def get_report():
    data = pd.read_sql('select dt,week,dau,click,view,play from report order by dt desc limit 30',con=engine).values.tolist()
    data_list = []
    for i in data:
        if i[1]!=7:
            data_list.append(f'<tr align="center"><td>{str(i[0])+"-星期"+str(i[1])}</td><td>{str(i[2])+"人"}</td><td>{str(i[3])+"次"}</td><td>{str(i[4])+"次"}</td><td>{str(int(i[5])//60)+"min"}</td></tr>')
        else:
            data_list.append(f'<tr><td align="center" colspan="5">本周日志报表</td></td></tr><tr align="center"><td>{str(i[0])+"-星期"+str(i[1])}</td><td>{str(i[2])+"人"}</td><td>{str(i[3])+"次"}</td><td>{str(i[4])+"次"}</td><td>{str(int(i[5])//60)+"min"}</td></tr>')
    text = f'''
    <table border="1" width="100%" bgcolor="#e9faff">
        <tr align="center">
            <th>日期</th>
            <th>日活</th>
            <th>人均点击次数</th>
            <th>人均曝光次数</th>
            <th>人均播放时长</th>
        </tr>{''.join(data_list)}
    </table>'''
    send(text,'每日日活统计报表')



def send(text,name):
    msg=MIMEText(text,'html','utf-8')
    msg['From']=formataddr(["bigdata_report",my_sender])
    msg['To']= ",".join(my_user)
    msg['Subject']=name
    server=smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
    server.login(my_sender, my_pass)
    server.sendmail(my_sender,my_user,msg.as_string())
    server.quit()

get_report()
