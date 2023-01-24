#!/usr/bin/env python

import sys
from random import choice
# from argparse import ArgumentParser, FileType
# from configparser import ConfigParser
from confluent_kafka import Producer
from query import executeQuery
from datetime import datetime
import json
def fun(ob):
    return str(ob)
    

if __name__ == "__main__":
    # Parse the command line.
    # parser = ArgumentParser()
    # parser.add_argument('config_file', type=FileType('r'))
    # args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # config_parser = ConfigParser()
    # config_parser.read_file(args.config_file)
    # config = dict(config_parser['default'])

    # Create Producer instance
    producer = Producer({"bootstrap.servers":"localhost:9092",'socket.connection.setup.timeout.ms':100000,"message.max.bytes":1000000000,'queue.buffering.max.kbytes':2147483647,'queue.buffering.max.ms':10000,'message.copy.max.bytes':1000000000,'receive.message.max.bytes':2147483647})

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(err, msg):
        if err:
            print('ERROR: Message failed delivery: {}'.format(err))
        else:
            # print("Produced event to topic {topic}: key = {key:12} value = {value:12}".format(
            #     topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
            # print(msg.value().decode('utf-8'))
            print("Data Sent")

    # Produce data by selecting random values from these lists.
    topic = "purchases"
    user_ids = ['eabara', 'jsmith', 'sgarcia', 'jbernard', 'htanaka', 'awalther']
    products = ['book', 'alarm clock', 't-shirts', 'gift card', 'batteries']
    tableName="to_test_kafka"
    queries=[f"SELECT * from {tableName} where id<=500000 ORDER BY id ASC",f"SELECT * from {tableName} where id>500000 ORDER BY id ASC"]
    colunms=executeQuery(f"SELECT column_name,data_type FROM information_schema.columns WHERE table_schema = 'public' AND table_name = '{tableName}'","actalyst_demo_1")
    # for i in queries:
    #     data=executeQuery(i,"actalyst_demo_1")
    #     datatokafka=json.dumps({"table_name":tableName,"colunms":colunms,"data":data},default=fun)
    #     producer.produce(topic,datatokafka, callback=delivery_callback)
    #     producer.poll(10000)
    #     producer.flush()
    data=executeQuery(f"SELECT * from {tableName}",'actalyst_demo_1')
    datatokafka=json.dumps({"table_name":tableName,"colunms":colunms,"data":data},default=fun).encode('utf8')
    print(sys.getsizeof(datatokafka))
    producer.produce(topic,datatokafka, callback=delivery_callback)
    producer.poll(10000)
    producer.flush()