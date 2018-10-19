#!/usr/bin/python
# coding:utf-8

from kafka import KafkaConsumer
import json
import requests
import os

TOPIC               = "PREPROCESS_RESULT_QUEUE"       #取数据的topic(类似于队列)
GROUP_ID            = "group_test1"               #用于标识数据的消费者，相同的group_id会共取同一份数据，如果需要完整数据建议group_id不要与其他人重复(建议使用身份证号或者工号)
BOOTSTRAP_SERVERS   =['10.255.65.3:9092']  #取数据服务器地址
DATA_SIMPLE         ="/home/authenticate/yara/simple/"   #下载样本的目录
#auto_offset_reset='earliest'               #从最早的数据开始取
'''
auto_offset_reset:参数说明
earliest 
当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，从头开始消费 
latest 
当各分区下有已提交的offset时，从提交的offset开始消费；无提交的offset时，消费新产生的该分区下的数据
'''
def kafka_simple():
    consumer = KafkaConsumer(TOPIC,group_id=GROUP_ID,bootstrap_servers=BOOTSTRAP_SERVERS)
    for message in consumer:
        data = json.loads(message.value)
        url = data[u'data'][u'download_url']
        file_name = url.split('/')[-1]

        r = requests.get(url)
        if r.status_code != 200:
            continue
        os.chdir(DATA_SIMPLE)
        with open(file_name, "wb") as code:
            code.write(r.content)

if __name__ == '__main__':
    kafka_simple()