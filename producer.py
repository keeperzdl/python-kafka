#!/usr/bin/python
# coding:utf-8

from confluent_kafka import Producer
import time
# import logging as log
# log.basicConfig(level=log.DEBUG)

producer = Producer({"bootstrap.servers":'10.255.65.3:9092',"message.max.bytes":1024*1024*100,})
#"compression.type":"gzip"

def delivery_report(err, msg):
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} [{}]'.format(msg.topic(), msg.partition()))
producer.poll(0)
for msg in range(500):
    producer.produce('test_test', msg,callback=delivery_report,key=key)
    print msg
producer.flush()