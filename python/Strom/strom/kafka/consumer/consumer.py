""" Kafka Consumer """
from pykafka import KafkaClient

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class Consumer():
    """ Simple balanced kafka consumer. Accepts kafka url string and topic name byte-string. (Optional) Time in ms to stay active. """
    def __init__(self, url, topic, timeout=-1):
        """ Init requires kafka url:port, topic name, and timeout for listening. """
        self.client = KafkaClient(hosts=url, zookeeper_hosts=None, use_greenlets=False)
        self.topic = self.client.topics[topic]
        self.consumer = self.topic.get_balanced_consumer(
            consumer_group=b'strom',
            zookeeper_connect=None,
            num_consumer_fetchers=1,
            reset_offset_on_start=False,
            auto_commit_enable=True,
            auto_commit_interval_ms=30000, #tweak
            queued_max_messages=200, #tweak
            consumer_timeout_ms=timeout,
            auto_start=False,
            use_rdkafka=False,
            fetch_min_bytes=1, #tweak
            fetch_message_max_bytes=1048576, #tweak
            fetch_wait_max_ms=100) #tweak
            # NOTE: may be quicker w/ alt. options

    def consume(self):
        """ Listen time determinied by 'timeout' param given on init. Compression options: 'snappy', 'gzip', None. """
        # NOTE: TODO Check diffs b/w for-loop and consumer.consume()
        self.consumer.start() #auto-start
        for msg in self.consumer:
            if msg is not None:
                print(str(msg.value) + ": {}".format(msg.offset))
