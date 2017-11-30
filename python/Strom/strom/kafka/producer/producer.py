""" Kafka Producer """
from pykafka import KafkaClient
from pykafka.utils.compression import Compression

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Producer():
    """ Simple kafka producer """
    def __init__(self, url, topic):
        self.client = KafkaClient(hosts=url, zookeeper_hosts=None, use_greenlets=False)
        self.topic = self.client.topics[topic]
        self.producer = self.topic.get_producer(delivery_reports=True, use_rdkafka=False)
        self.count = 0

    def _snappy(self, data):
        cdata = Compression.encode_snappy(data, xerial_compatible=True, xerial_blocksize=32768)
        return cdata

    def produce(self, msg):
        """ Produce to given topic and log e. 20k msg. """
        smsg = _snappy(msg) #NOTE
        bcount = str(self.count).encode()
        self.producer.produce(smsg, partition_key=bcount)
        self.count += 1
        if count == 20000:
            while True:
                try:
                    msg, exc = producer.get_delivery_report(block=False)
                    if exc is not None:
                        print("Delivery Fail: {}: {}".format(msg.partition_key, repr(exc))) #replace w/ logger
                    else:
                        print("Success: {}".format(msg.partition_key))
                except:
                    pass #TEMP
