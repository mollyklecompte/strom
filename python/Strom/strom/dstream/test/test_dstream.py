import unittest

from Strom.strom import DStream


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

        tag_name = "Really good sensor"
        self.dstream._add_tag(tag_name)
        self.assertEqual(tag_name, self.dstream["tags"][0])

        fk = "key to the city"
        self.dstream._add_fk(fk)
        self.assertEqual(fk, self.dstream["foreign_keys"][0])

        fake_filter = {"func_name":"Make all values 0"}
        self.dstream._add_filter(fake_filter)
        self.assertEqual(fake_filter, self.dstream["filters"][0])

        fake_dparam = {"measure":"viscosity", "drule":"max of mins"}
        self.dstream._add_derived_param(fake_dparam)
        self.assertEqual(fake_dparam, self.dstream["dparam_rules"][0])

        fake_event_name = "My birthday"
        fake_event = {"param":"viscocity", "threshold":"too viscous"}
        self.dstream._add_event(fake_event_name, fake_event)
        self.assertEqual(fake_event, self.dstream["event_rules"][fake_event_name])







if __name__ == "__main__":
    unittest.main()
