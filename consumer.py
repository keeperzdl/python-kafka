#!/usr/bin/env.python
# -*- coding: utf-8 -*-

from pykafka import KafkaClient
from system_conf import *

from confluent_kafka import Consumer, KafkaError

c = Consumer({
    'bootstrap.servers': 'mybroker',
    'group.id': 'mygroup',
    'default.topic.config': {
        'auto.offset.reset': 'smallest'
    }
})
c.subscribe(['mytopic'])
while True:
    msg = c.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        if msg.error().code() == KafkaError._PARTITION_EOF:
            continue
        else:
            print(msg.error())
            break
    print('Received message: {}'.format(msg.value().decode('utf-8')))

c.close()