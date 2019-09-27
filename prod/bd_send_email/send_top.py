import smtplib
from email.mime.text import MIMEText
from email.utils import formataddr
import os
import vthread


my_sender = '17612472355@baidaweb.cn'
my_pass = 'Lishuo4322'
my_user = ['wangjinglei@baidaweb.cn','17612472355@baidaweb.cn','wanggang@baidaweb.cn','lizhiqiao@baidaweb.cn','lemon@baidaweb.cn','Zichang@baidaweb.cn']
@vthread.pool(3)
def send(text,name):
    msg=MIMEText(text,'html','utf-8')    
    msg['From']=formataddr(["bigdata_report",my_sender])
    msg['To']= ",".join(my_user)
    msg['Subject']=name
    server=smtplib.SMTP_SSL("smtp.exmail.qq.com", 465)
    server.login(my_sender, my_pass)
    server.sendmail(my_sender,my_user,msg.as_string())
    server.quit()


def main():
    path = '/mnt/bigdata_workspace/pyspark/prod/bd_send_email/top_html/' 
    for i in os.listdir(path):
        if 'html' in i:
            name = i.split('.')[0]
            with open(path+i,'r') as f:
                text = f.read()
            send(text,name)
main()

