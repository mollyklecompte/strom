""" Kafka Producer """
from pykafka import KafkaClient

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Producer():
    """ Simple kafka producer, accepts kafka url string and topic name byte-string. """
    def __init__(self, url, topic):
        self.client = KafkaClient(hosts=url, use_greenlets=False)
        self.topic = self.client.topics[topic]
        self.producer = self.topic.get_producer(delivery_reports=True, use_rdkafka=False)
        self.count = 0

    def produce(self, dmsg):
        """ Produce to given topic w/ partition_key and log e. 1k msg. """
        bcount = str(self.count).encode()
        self.producer.produce(dmsg, partition_key=bcount)
        self.count += 1
        if self.count == 1000:
            while True:
                try:
                    msg, exc = self.producer.get_delivery_report(block=False)
                    if exc is not None:
                        print("Delivery Fail: {}: {}".format(msg.partition_key, repr(exc))) #replace w/ logger
                    else:
                        print("Success: {}".format(msg.partition_key))
                except:
                    pass #TEMP
