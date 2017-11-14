"""Abstract class for our transform functions"""

from abc import ABCMeta, abstractmethod


class Transformer(object):
    __metaclass__ = ABCMeta
    def __init__(self):
        self.data = {}
        self.params = {}

    def add_measure(self, measure_name, measure_data):
        """Method to select the parameter[s] for transformation"""
        self.data[measure_name] = measure_data

    def load_measures(self, measure_dict):
        """Load a dict of measures into transformation"""
        for key, value in measure_dict.items():
            self.add_measure(key, value)

    def load_params(self, params):
        """Method for setting the parameters of the transformation"""
        self.params = params

    def get_defaults(self):
        return self.params

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

