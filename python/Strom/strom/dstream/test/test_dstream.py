import unittest
import uuid

from strom.dstream.dstream import DStream


class TestDStream(unittest.TestCase):
    def setUp(self):
        self.dstream = DStream()

    def test_init(self):
        init_keys = ['stream_name', 'user_description', 'version', 'stream_token', 'source_key', 'template_id', 'storage_rules', 'ingest_rules', 'engine_rules', 'timestamp', 'measures', 'fields', 'user_ids', 'tags', 'foreign_keys', 'filters', 'dparam_rules', 'event_rules', 'data_rules']
        for key in init_keys:
            print(self.dstream.keys())
            self.assertIn(key, self.dstream.keys())

    def testadd_methods(self):
        self.assertIsInstance(self.dstream["stream_token"], uuid.UUID)

        m_name = "viscosity"
        m_dtype = "float"
        self.dstream.add_measure(m_name, m_dtype)
        self.assertTrue(m_name in self.dstream["measures"].keys())
        self.assertEqual(self.dstream["measures"][m_name]["dtype"], m_dtype)

        f_name = "strawberry"
        self.dstream.add_field(f_name)
        self.assertTrue(f_name in self.dstream["fields"].keys())

        uid_name = "my_id"
        self.dstream.add_user_id(uid_name)
        self.assertTrue(uid_name in self.dstream["user_ids"].keys())

        tag_name = "Really good sensor"

        self.dstream.add_tag(tag_name)
        self.assertIn(tag_name, self.dstream["tags"])

        fk = "key to the city"
        self.dstream.add_fk(fk)
        self.assertTrue({fk:None} in self.dstream["foreign_keys"])

        fake_filter = {"func_name":"Make all values 0"}
        self.dstream.add_filter(fake_filter)
        self.assertEqual(fake_filter, self.dstream["filters"][0])

        fake_dparam = {"measure":"viscosity", "drule":"max of mins"}
        self.dstream.add_derived_param(fake_dparam)
        self.assertEqual(fake_dparam, self.dstream["dparam_rules"][0])

        fake_event_name = "My birthday"
        fake_event = {"param":"viscosity", "threshold":"too viscous"}
        self.dstream.add_event(fake_event_name, fake_event)
        self.assertEqual(fake_event, self.dstream["event_rules"][fake_event_name])

        old_version = self.dstream["version"]
        self.dstream.publish_version()
        self.assertEqual(old_version+1, self.dstream["version"])

        fake_mapping = ["fake","mapping","list"]
        self.dstream.add_data_rules(fake_mapping)
        self.assertEqual(fake_mapping, self.dstream["data_rules"])

    def test_load_from_json(self):
        test_dict = {"stream_token":"foo", "version":900}
        self.dstream.load_from_json((test_dict))
        self.assertEqual(test_dict["version"], self.dstream["version"])
        self.assertIsInstance(self.dstream["stream_token"], uuid.UUID)

    def test_filter(self):
        self.dstream.add_filter()



if __name__ == "__main__":
    unittest.main()
