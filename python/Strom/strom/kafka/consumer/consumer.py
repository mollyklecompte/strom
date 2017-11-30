""" Kafka Consumer """
from pykafka import KafkaClient
import pykafka.utils.compression as Compression

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Consumer():
    """ Simple balanced kafka consumer. """
    def __init__(self, url, topic, zk_url):
        """ Init requires kafka url:port, topic name, and zookeeper url:port. """
        self.client = KafkaClient(hosts=url, zookeeper_hosts=None, use_greenlets=False)
        self.topic = self.client.topics[topic]
        self.consumer = self.topic.get_balanced_consumer(
            consumer_group=b'test',
            num_consumer_fetchers=1,
            reset_offset_on_start=False,
            zookeeper_connect=zk_url,
            auto_commit_enable=True,
            auto_commit_interval_ms=60000,
            queued_max_messages=2000,
            consumer_timeout_ms=1,
            auto_start=False,
            use_rdkafka=False)  #may be quicker w/ alt. options

    def _decompress_if_snappy(self, msg):
        """ Decompress stream if snappy compression is found. """
        msg_unpkg = Compression.decode_snappy(msg)
        return msg_unpkg

    def listen(self):
        """ Actively 'listen' on given topic. Check consumer_timeout_ms option in init. """
        self.consumer.start() #auto-start
        for msg in self.consumer:
            if msg is not None:
                print(msg.value)#NOTE: TEMP

    def consume(self):
        """ Collect all new messages in a topic at once. """
        self.consumer.start() #auto-start
        msg = self.consumer.consume()
        if msg is not None:
            try:
                decompressed = _decompress_if_snappy(msg)
            except:
                self.consumer.commit_offsets()
                return msg
            else:
                self.consumer.commit_offsets()
                return decompressed
