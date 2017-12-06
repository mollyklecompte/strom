import unittest
from strom.kafka.topics.checker import TopicChecker
from strom.kafka.producer.producer import Producer

class TestTopicChecker(unittest.TestCase):
    def setUp(self):
        self.tc = TopicChecker('127.0.0.1:9092')
        self.producer = Producer('127.0.0.1:9092', b'temp')

    def test_topiccheck_w_new(self):
        startnum = self.tc._get_len()
        self.producer = Producer('127.0.0.1:9092', b'topictest')
        endnum = self.tc._get_len()
        topics = self.tc._list()
        self.assertNotEqual(endnum, startnum)
        self.assertIn(b'topictest', topics)

    def test_topiccheck_init(self):
        self.assertIsInstance(self.tc, TopicChecker)
        self.assertIsInstance(self.producer, Producer)

if __name__ == '__main__':
    unittest.main()
