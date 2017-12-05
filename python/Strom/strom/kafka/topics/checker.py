from pykafka import KafkaClient

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

class TopicChecker():
    """ Class that implements callback functions for current or new topics. """
    def __init__(self, url):
        self.client = KafkaClient(hosts=url)
        self.topics = self.client.topics

    def _update(self):
        self.client.update_cluster()

    def _list(self):
        self._update()
        return self.topics

    def _get_len(self):
        res = self._list()
        return len(res)

    def _check_start(self, callback, pause):
        self.check(callback, pause)

    def check(self):
        count = len(self.topics)
        counting = count
        while count == counting:
            counting = self._get_len()
        print(count)
        print(counting)
