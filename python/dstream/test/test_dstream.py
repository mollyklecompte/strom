import unittest
from dstream.dstream import DStream


class TestDStream(unittest.TestCase):
    def setUp(self):
        self.dstream = DStream()

    def test_init(self):
        init_keys = ['device_id', 'version', 'stream_token', 'timestamp',
                     'measures', 'fields', 'user_ids', 'tags', 'foreign_keys',
                     'filters', 'dparam_rules', 'event_rules']
        self.assertIsInstance(self.dstream, DStream)
        self.assertEqual(init_keys, list(self.dstream.keys()))

    def test_add_methods(self):
        m_name = "viscosity"
        m_dtype = "float"
        self.dstream._add_measure(m_name, m_dtype)
        self.assertTrue(m_name in self.dstream["measures"].keys())
        self.assertEqual(self.dstream["measures"][m_name]["dtype"], m_dtype)

        f_name = "strawberry"
        self.dstream._add_field(f_name)
        self.assertTrue(f_name in self.dstream["fields"].keys())

        uid_name = "my_id"
        self.dstream._add_user_id(uid_name)
        self.assertTrue(uid_name in self.dstream["user_ids"].keys())




if __name__ == "__main__":
    unittest.main()
