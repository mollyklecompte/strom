""" Kafka Consumer """
from pykafka import KafkaClient
import pykafka.utils.compression as Compression

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Consumer():
    """ Simple balanced kafka consumer. """
    def __init__(self, url, topic, timeout):
        """ Init requires kafka url:port, topic name, and timeout for listening. """
        self.client = KafkaClient(hosts=url, zookeeper_hosts=None, use_greenlets=False)
        self.topic = self.client.topics[topic]
        self.consumer = self.topic.get_balanced_consumer(
            consumer_group=b'strom',
            num_consumer_fetchers=1,
            reset_offset_on_start=False,
            #zookeeper_connect=zk_url,
            auto_commit_enable=True,
            auto_commit_interval_ms=60000,
            queued_max_messages=2000,
            consumer_timeout_ms=(timeout * 1000),
            auto_start=False,
            use_rdkafka=False)  # NOTE: may be quicker w/ alt. options

    def _snappy_decompress(self, msg):
        msg_unpkg = Compression.decode_snappy(msg)
        return msg_unpkg
    def _gzip_decompress(self, msg):
        msg_unpkg = Compression.decode_gzip(msg)
        return msg_unpkg

    def consume(self, compression=None):
        """ Listen time determinied by 'timeout' param given on init. Compression options: 'snappy', 'gzip', None. """
        # NOTE: TODO Check diffs b/w for-loop and consumer.consume()
        self.consumer.start() #auto-start
        for msg in self.consumer:
            if msg is not None:
                if compression == "snappy":
                    com_msg = self._snappy_decompress(msg.value)
                elif compression == "gzip":
                    com_msg = self._gzip_decompress(msg.value)
                else:
                    com_msg = msg.value
                print(str(com_msg) + ": {}".format(msg.offset))
