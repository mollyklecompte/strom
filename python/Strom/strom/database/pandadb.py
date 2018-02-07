""" abstract database class w/ default basic pandas sql statements"""
import pandas
from abc import ABCMeta, abstractmethod

__version__='0.0.1'
__author__='Adrian Agnic'

class PandaDB(metaclass=ABCMeta):

    def __init__(self, conn):
        self.conn = conn
        super().__init__()

    def select(self, query, df=None):
        """ standard sql parametrized select """
        raise NotImplementedError()
        if not df:
            pass
        pass

    def create(self, query, df=None):
        """ standard sql parametrized create """
        raise NotImplementedError()
        if not df:
            pass
        pass

    def read(self, query, df=None):
        """ standard sql parametrized read """
        raise NotImplementedError()
        if not df:
            pass
        pass

    def update(self, query, df=None):
        """ standard sql parametrized update """
        raise NotImplementedError()
        if not df:
            pass
        pass

    def delete(self, query, df=None):
        """ standard sql parametrized delete """
        raise NotImplementedError()
        if not df:
            pass
        pass

    def connect(self):
        """ open database connection """
        raise NotImplementedError()
        pass

    def close(self):
        """ close database connection """
        raise NotImplementedError()
        pass
