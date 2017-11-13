"""Abstract class for our transform functions"""

from abc import ABCMeta, abstractmethod


class Transformer(object):
    __metaclass__ = ABCMeta
    def __init__(self):
        self.data = {}

    def add_measure(self, measure_name, measure_data):
        """Method to select the parameter[s] for transformation"""
        self.data[measure_name] = measure_data


    @abstractmethod
    def set_params(self, params):
        """Method for setting the parameters of the transformation"""
        raise NotImplementedError("subclass must implement this abstract method.")

    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")