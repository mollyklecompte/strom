import unittest

from strom.engine.buffer import Buffer


class TestBuffer(unittest.TestCase):
    def setUp(self):
        self.tester = [1, 2, 3, 4, 5]
        self.buffer = Buffer()
        self.buffer_roll = Buffer(1)

    def test_init(self):
        self.assertEqual(self.buffer_roll.rolling_window, -1)
        self.assertTrue(self.buffer.rolling_window is None)

    def test_reset(self):
        for i in range(5):
            self.buffer.append(self.tester[i])
            self.buffer_roll.append(self.tester[i])

        self.buffer.reset()
        self.assertEqual(len(self.buffer), 0)

        self.buffer_roll.reset()
        self.assertEqual(len(self.buffer_roll), 1)
        self.assertIn(5, self.buffer_roll)
