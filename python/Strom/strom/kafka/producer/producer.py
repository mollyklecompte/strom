""" Kafka Producer """
from pykafka import KafkaClient
import pykafka.utils.compression as Compression

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
    def _gzip(self, data):
        cdata = Compression.encode_gzip(data)
        return cdata
    def _lz4(self, data):
        cdata = Compression.encode_lz4_old_kafka(data)
        return cdata

    def produce(self, compression, msg):
        """
        \b
        Produce to given topic and log e. 20k msg.
        Expects type of compression and message data.
        Compression options: snappy, gzip, lz4, none
        """
        if compression == "snappy":
            com_msg = _snappy(msg)
        elif compression == "gzip":
            com_msg = _gzip(msg)
        elif compression == "lz4":
            com_msg = _lz4(msg)
        else:
            com_msg = msg
        bcount = str(self.count).encode()
        self.producer.produce(com_msg, partition_key=bcount)
        self.count += 1
        if self.count == 20000:
            while True:
                try:
                    msg, exc = producer.get_delivery_report(block=False)
                    if exc is not None:
                        print("Delivery Fail: {}: {}".format(msg.partition_key, repr(exc))) #replace w/ logger
                    else:
                        print("Success: {}".format(msg.partition_key))
                except:
                    pass #TEMP
