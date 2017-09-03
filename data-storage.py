# - read from kafka
# - wirte to cassandra
# - get message from kafka write to cassandra
import atexit
import json

from cassandra.cluster import Cluster
from kafka import KafkaConsumer

import argparse
import logging

from kafka.errors import KafkaError

logger_format = '%(asctime)-15s %(message)s'
logging.basicConfig(format=logger_format)
logger = logging.getLogger('data-storage')
logger.setLevel(logging.DEBUG)

topic_name = 'stock-analyzer'
kafka_broker = '127.0.0.1:9092'
cassandra_broker = '127.0.0.1'
key_space = 'stock'
data_table = 'stock_price'


def persist_data(stock_data, cassandra_session):
    try:
        logger.debug('Start to persist data to cassandra %s', stock_data)
        parsed = json.loads(stock_data)[0]
        symbol = parsed.get('StockSymbol')
        price = float(parsed.get('LastTradePrice'))
        tradetime = parsed.get('LastTradeDateTime')
        statement = "INSERT INTO %s (stock_symbol, trade_time, trade_price) VALUES ('%s', '%s', %f)" % (
            data_table, symbol, tradetime, price)
        cassandra_session.execute(statement)
        logger.info(
            'Persistend data to cassandra for symbol: %s, price: %f, tradetime: %s' % (symbol, price, tradetime))
    except Exception:
        logger.error('Failed to persist data to cassandra %s', stock_data)


def shutdown_hook(consumer, session):
    """
    a shutdown hook to be called before the shutdown
    :param consumer: instance of a kafka consumer
    :param session: instance of a cassandra session
    :return: None
    """
    try:
        logger.info('Closing Kafka Consumer')
        consumer.close()
        logger.info('Kafka Consumer closed')
        logger.info('Closing Cassandra Session')
        session.shutdown()
        logger.info('Cassandra Session closed')
    except KafkaError as kafka_error:
        logger.warn('Failed to close Kafka Consumer, caused by: %s', kafka_error.message)
    finally:
        logger.info('Existing program')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('topic_name', help='the topic of kafka to read')
    parser.add_argument('kafka_broker', help='the ip of kafka broker')
    parser.add_argument('cassandra_broker', help='the ip of cassandra broker')
    parser.add_argument('keyspace', help='the keyspace to use in cassandra')
    parser.add_argument('data_table', help='the table to use')

    args = parser.parse_args()
    topic_name = args.topic_name
    kafka_broker = args.kafka_broker
    cassandra_broker = args.cassandra_broker
    key_space = args.keyspace
    data_table = args.data_table

    consumer = KafkaConsumer(
        topic_name,
        bootstrap_servers=kafka_broker
    )

    cassandra_cluster = Cluster(
        contact_points=cassandra_broker.split(',')
    )

    session = cassandra_cluster.connect()

    session.execute(
        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'} "
        "AND durable_writes = 'true'" % key_space)
    session.set_keyspace(key_space)
    session.execute(
        "CREATE TABLE IF NOT EXISTS %s (stock_symbol text, trade_time timestamp, trade_price float, PRIMARY KEY (stock_symbol,trade_time))" % data_table)

    atexit.register(shutdown_hook, consumer, session)

    for msg in consumer:
        persist_data(msg.value, session)
