import unittest
from Strom.strom.transform.transform import Transformer

class TestTransformer(unittest.TestCase):
    def setUp(self):
        self.transformer = Transformer()

    def test_init(self):
        self.assertIsInstance(self.transformer.data, dict)
        self.assertIsInstance(self.transformer.params, dict)

    def test_measures(self):
        test_measures = {"viscosity":{"val":range(5), "dtype":"int"}, "height":{"val":range(5,10), "dtype":"int"}}
        self.transformer.load_measures(test_measures)
        for key in test_measures.keys():
            self.assertIn(key, self.transformer.data)



if __name__ == "__main__":
    unittest.main()
