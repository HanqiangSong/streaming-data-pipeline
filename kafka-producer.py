# - connect to any kafka broker
# - fetch stock price everty second

from kafka import KafkaProducer

import argparse
import logging

# - logging
logging.basicConfig()
logger = logging.getLogger("data-producer")

# - set logging level
logger.setLevel(logging.DEBUG)

kafka_broker = '127.0.0.1:9092'
topic = 'test'

if __name__ == '__main__':
    # - setup commandline argument
    parser = argparse.ArgumentParser()
    parser.add_argument('kafka_broker', help='the location of kafka broker')
    parser.add_argument('topic', help='the topic of kafka')

    args = parser.parse_args()
    topic = args.topic
    kafka_broker = args.kafka_broker

    # logger.debug('stok symbol is %s' % symbol)
    producer = KafkaProducer(
        bootstrap_servers=kafka_broker
    )
    msg = 'Send message to kafka.'

    producer.send(topic=topic, value=msg)
