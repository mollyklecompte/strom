import unittest
from strom.kafka.producer.producer import Producer
from strom.kafka.consumer.consumer import Consumer

class TestKafka(unittest.Testcase):
    def setUp(self):
        self.producer = Producer('127.0.0.1:9092', b'test')
        self.consumer = Consumer('127.0.0.1:9092', b'test')

    def test_kafka(self):
        pass

if __name__ == '__main__':
    unittest.main()
