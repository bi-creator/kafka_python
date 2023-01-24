
# import sys
# from argparse import ArgumentParser, FileType
# from configparser import ConfigParser
from confluent_kafka import Consumer, OFFSET_BEGINNING
import json
from query import executeQuery,insertintodbcsvwriter
if __name__ == "__main__":
    # Parse the command line.
    # parser = ArgumentParser()
    # parser.add_argument('config_file', type=FileType('r'))
    # parser.add_argument('--reset', action='store_true')
    # args = parser.parse_args()

    # Parse the configuration.
    # See https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
    # config_parser = ConfigParser()
    # config_parser.read_file(args.config_file)
    # config = dict(config_parser['default'])
    # config.update(config_parser['consumer'])

    # Create Consumer instance
    consumer = Consumer({"bootstrap.servers":"localhost:9092",'session.timeout.ms':90000,'socket.connection.setup.timeout.ms':100000,"auto.offset.reset":"earliest","group.id":"manjith",'receive.message.max.bytes':2147483647,'max.partition.fetch.bytes':1000000000})

    # Set up a callback to handle the '--reset' flag.
    def reset_offset(consumer, partitions):
        # if args.reset:
        #     for p in partitions:
        #         p.offset = OFFSET_BEGINNING
        #     consumer.assign(partitions)
        pass

    # Subscribe to topic
    topic = "purchases"
    consumer.subscribe([topic], on_assign=reset_offset)

    # Poll for new messages from Kafka and print them.
    datarecived=0
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                # Initial message consumption may take up to
                # `session.timeout.ms` for the consumer group to
                # rebalance and start consuming
                print("Waiting...")
            elif msg.error():
                print("ERROR: %s".format(msg.error()))
            else:
                # Extract the (optional) key and value, and print.

                # print("Consumed event from topic {topic}: key = {key:12} value = {value:12}".format(
                #     topic=msg.topic(), key=msg.key().decode('utf-8'), value=msg.value().decode('utf-8')))
                dataFromKafka=json.loads(msg.value())
                # print(dataFromKafka)
                print("data Reviced")
                datarecived=1
                # print(type(dataFromKafka["colunms"]))
                exisistingTables=[table[0] for table in executeQuery("SELECT table_name FROM information_schema.tables WHERE table_schema='public'","actalyst_demo_1_ubntu")]
                colunm_names=[i[0] for i in dataFromKafka["colunms"]]
                print(colunm_names)
                if(dataFromKafka['table_name'] in exisistingTables):
                    insertintodbcsvwriter("actalyst_demo_1_ubntu",dataFromKafka['data'],colunm_names,dataFromKafka['table_name'])
                else:
                    s=''
                    for i,j in dataFromKafka["colunms"][1:]:
                        s=s+f""""{i}" {j}"""+','
                    s=s[:-1]
                    query=f"""CREATE TABLE public.{dataFromKafka['table_name']}
                    (   
                        ID  SERIAL PRIMARY KEY,
                        {s}
                        
                    )
        
                    """
                    print(query)
                    # print(dataFromKafka['data'])
                    executeQuery(query,"actalyst_demo_1_ubntu")
                    insertintodbcsvwriter("actalyst_demo_1_ubntu",dataFromKafka['data'],colunm_names,dataFromKafka['table_name'])
    except KeyboardInterrupt:
        pass
    finally:
        # Leave group and commit final offsets
        consumer.close()
