import unittest
import uuid
from strom.dstream.dstream import DStream


class TestDStream(unittest.TestCase):
    def setUp(self):
        self.dstream = DStream()

    def test_init(self):
        init_keys = ['stream_name', 'version', 'stream_token', 'sources', 'storage_rules',
                     'ingest_rules', 'engine_rules', 'timestamp', 'measures', 'fields',
                     'user_ids', 'tags', 'foreign_keys', 'filters', 'dparam_rules', 'event_rules']
        self.assertEqual(init_keys, list(self.dstream.keys()))

    def test_add_methods(self):
        self.assertIsInstance(self.dstream["stream_token"], uuid.UUID)

        source = {"kafka":"On the shore"}
        self.dstream._add_source("kafka", "On the shore")
        self.assertEqual(self.dstream["sources"], source)
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
        self.assertIn(tag_name, self.dstream["tags"])

        fk = "key to the city"
        self.dstream._add_fk(fk)
        self.assertTrue(fk in self.dstream["foreign_keys"].keys())

        fake_filter = {"func_name":"Make all values 0"}
        self.dstream._add_filter(fake_filter)
        self.assertEqual(fake_filter, self.dstream["filters"][0])

        fake_dparam = {"measure":"viscosity", "drule":"max of mins"}
        self.dstream._add_derived_param(fake_dparam)
        self.assertEqual(fake_dparam, self.dstream["dparam_rules"][0])

        fake_event_name = "My birthday"
        fake_event = {"param":"viscosity", "threshold":"too viscous"}
        self.dstream._add_event(fake_event_name, fake_event)
        self.assertEqual(fake_event, self.dstream["event_rules"][fake_event_name])

        old_version = self.dstream["version"]
        self.dstream._publish_version()
        self.assertEqual(old_version+1, self.dstream["version"])

    def test_load_from_json(self):
        test_dict = {"stream_token":"foo", "version":900}
        self.dstream.load_from_json((test_dict))
        self.assertEqual(test_dict["version"], self.dstream["version"])
        self.assertIsInstance(self.dstream["stream_token"], uuid.UUID)


    def test_define(self):
        storage_rules = {"store":"yes"}
        ingestion_rules = {"frequency":"Not too fast, you don't want to upset your tummy"}
        source = {"kafka":"On the shore"}
        measure_list = [("viscosity","float"),("location","float"),("height","inches")]
        field_names = ["low", "lie", "athenry"]
        user_ids = ["Number1","112358"]
        tag = ["Stop making sesnors", "Sixth sensor"]
        filter_list = [{"func_name":"Make all values 0"}, {"func_name":"delete the data"}]
        dparam_list = [{"measure":"viscosity", "drule":"max of mins"},
                       {"measure":"softness", "drule":"return max floof"}]
        event_list = [("My birthday", {"param":"viscosity", "threshold":"too viscous"})]

        self.dstream.define_dstream(storage_rules, ingestion_rules, source, measure_list, field_names,
                                    user_ids, tag, filter_list, dparam_list, event_list)

        self.assertEqual(storage_rules, self.dstream["storage_rules"])
        self.assertEqual(ingestion_rules, self.dstream["ingest_rules"])

        self.assertTrue("viscosity" in self.dstream["measures"].keys())
        self.assertEqual(self.dstream["measures"]["viscosity"]["dtype"], "float")

        self.assertEqual(len(self.dstream["fields"].keys()), 3)
        self.assertTrue("low" in self.dstream["fields"].keys())

        self.assertEqual(len(self.dstream["user_ids"].keys()), 2)
        self.assertTrue("Number1" in self.dstream["user_ids"].keys())

        self.assertEqual(len(self.dstream["tags"]), 2)

        self.assertEqual(len(self.dstream["filters"]), 2)
        self.assertEqual(len(self.dstream["dparam_rules"]), 2)
        self.assertTrue("My birthday" in self.dstream["event_rules"])

        self.assertEqual(self.dstream["version"], 1)


if __name__ == "__main__":
    unittest.main()
