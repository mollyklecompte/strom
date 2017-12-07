"""Abstract class for our transform functions"""

from abc import ABCMeta, abstractmethod
from strom.utils.logger.logger import logger


class Transformer(object):
    __metaclass__ = ABCMeta
    def __init__(self):
        logger.debug("initializing transformer")
        self.data = {}
        self.params = {}

    def add_measure(self, measure_name, measure_data):
        """Method to select the parameter[s] for transformation"""
        self.data[measure_name] = measure_data
        logger.debug("added measure %s" % (measure_name))

    def load_measures(self, measure_dict):
        """Load a dict of measures into transformation"""
        logger.debug("added measure dict")
        for key, value in measure_dict.items():
            self.add_measure(key, value)

    @abstractmethod
    def load_params(self, params):
        """Method for setting the parameters of the transformation"""
        raise NotImplementedError("subclass must implement this abstract method.")

    @abstractmethod
    def get_params(self):
        """Method to return transformer's default parameters"""
        raise NotImplementedError("subclass must implement this abstract method.")


    @abstractmethod
    def transform_data(self):
        """Method to apply the transformation and return the transformed data"""
        raise NotImplementedError("subclass must implement this abstract method.")

