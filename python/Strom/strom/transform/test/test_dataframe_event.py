import unittest

from strom.transform.event import Event


class TestEvent(unittest.TestCase):
    def setUp(self):
        pass
    def test_init(self):
        e1 = Event()
        self.assertIsInstance(e1, Event)
        in2 = {"event_name": "Thanksgiving", "nothing": "gold", "event_context": "This is an american holiday"}
        e2 = Event(in2)
        self.assertIsInstance(e2, Event)
        self.assertEqual(e2["event_name"], in2["event_name"])
        self.assertNotIn("nothing", e2)

if __name__ == "__main__":
    unittest.main()
