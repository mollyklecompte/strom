""" Kafka Producer """
from pykafka import KafkaClient
from strom.utils.logger.logger import logger
from strom.utils.stopwatch import stopwatch as tk

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Producer():
    """ Simple kafka producer, accepts kafka url string and topic name byte-string. """
    def __init__(self, url, topic):
        self.client = KafkaClient(hosts=url, use_greenlets=False)
        logger.debug(topic)
        self.topic = self.client.topics[topic]
        logger.debug(self.topic)
        self.producer = self.topic.get_producer(delivery_reports=False, use_rdkafka=False)
        logger.debug("Producer init'ed")
        self.count = 0

    def produce(self, dmsg):
        """ Produce to given topic w/ partition_key and log e. 1k msg. """
        tk['Producer.produce'].start()
        bcount = str(self.count).encode()
        tk['Producer.produce : self.producer.produce'].start()
        self.producer.produce(dmsg, partition_key=bcount)
        tk['Producer.produce : self.producer.produce'].stop()
        logger.debug("Just produced a message")
        self.count += 1
        tk['Producer.produce'].stop()
        # if self.count == 1000:
        #     while True:
        #         try:
        #             msg, exc = self.producer.get_delivery_report(block=False)
        #             if exc is not None:
        #                 logger.warn("Kafka Producer Error: {} from {}".format(exc, msg.partition_key))
        #                 print("Delivery Fail: {}: {}".format(msg.partition_key, repr(exc))) #replace w/ logger
        #             else:
        #                 print("Success: {}".format(msg.partition_key))
        #         except:
        #             pass #TEMP
