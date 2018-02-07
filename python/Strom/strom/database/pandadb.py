""" abstract database class w/ default basic pandas sql statements"""
import pandas
from abc import ABCMeta, abstractmethod

__version__='0.0.1'
__author__='Adrian Agnic'

class PandaDB(metaclass=ABCMeta):

    def __init__(self, conn):
        self.conn = conn
        super().__init__()

    @abstractmethod
    def select(self, query, df=None):
        """ standard sql parametrized select """
        if not df:
            pass
        pass

    @abstractmethod
    def create(self, query, df=None):
        """ standard sql parametrized create """
        if not df:
            pass
        pass

    @abstractmethod
    def read(self, query, df=None):
        """ standard sql parametrized read """
        if not df:
            pass
        pass

    @abstractmethod
    def update(self, query, df=None):
        """ standard sql parametrized update """
        if not df:
            pass
        pass

    @abstractmethod
    def delete(self, query, df=None):
        """ standard sql parametrized delete """
        if not df:
            pass
        pass

    @abstractmethod
    def connect(self):
        """ open database connection """
        pass

    @abstractmethod
    def close(self):
        """ close database connection """
        pass
