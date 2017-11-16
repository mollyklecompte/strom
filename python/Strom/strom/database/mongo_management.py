"""Mongodb Manager Class

This class contains methods for inserting and reading from Mongodb
"""

from pymongo import MongoClient

__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"

# temporary config
config = {'mongo_host' : '10.156.86.156', 'mongo_port' : 27017, 'template_coll' : 'templates', 'derived_coll' : 'derived_params', 'event_coll' : 'events', 'db' : 'strom'}


class MongoManager(object):

    def __init__(self):
        self.client = MongoClient(config['mongo_host'], config['mongo_port'])
        self.db = self.client[config['db']]
        self.temp_coll_name = config['template_coll']
        self.temp_collection = self.db[self.temp_coll_name]
        self.derived_coll_name = config['derived_coll']
        self.derived_collection = self.db[self.derived_coll_name]
        self.event_coll_name = config['event_coll']
        self.event_collection = self.db[self.event_coll_name]

    def _insert(self, dstream, dtype):
        doc = dstream

        if dtype == 'template':
            inserted = self.temp_collection.insert_one(doc)
        elif dtype == 'derived':
            inserted = self.derived_collection.insert_one(doc)
        elif dtype == 'event':
            inserted = self.event_collection.insert_one(doc)
        else:
            raise ValueError('Invalid d-stream type.')

        if inserted.acknowledged is True:
            return inserted.inserted_id
        else:
            raise ValueError('Insert failed.')

    def _get_by_id(self, doc_id, dtype):
        if dtype == 'template':
            doc = self.temp_collection.find_one({"_id": doc_id})
        elif dtype == 'derived':
            doc = self.derived_collection.find_one({"_id": doc_id})
        elif dtype == 'event':
            doc = self.event_collection.find_one({"_id": doc_id})
        else:
            raise ValueError('Invalid d-stream type')

        if doc is not None:
            return doc

        else:
            raise ValueError('Document with id ' + doc_id + ' does not exist.')


