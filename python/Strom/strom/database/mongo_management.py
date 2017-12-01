"""Mongodb Manager Class

This class contains methods for inserting and reading from Mongodb
"""
from pymongo import MongoClient
from strom.utils.logger.logger import logger
from strom.utils.configer import configer as config

__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"


class MongoManager(object):
    def __init__(self):
        self.client = MongoClient(config['mongo_host'], int(config['mongo_port']))
        self.db = self.client[config['mongo_db']]
        self.temp_coll_name = config['template_coll']
        self.temp_collection = self.db[self.temp_coll_name]
        self.derived_coll_suffix = config['derived_coll_suf']
        self.event_coll_suffix = config['event_coll_suf']

    def insert(self, doc, dtype):
        token = doc["stream_token"]

        if dtype == 'template':
            inserted = self.temp_collection.insert_one(doc)
        elif dtype == 'derived':
            coll_name = '%s_%s' % (token, self.derived_coll_suffix)
            coll = self.db[coll_name]
            inserted = coll.insert_one({self.derived_coll_suffix: doc[self.derived_coll_suffix], "timestamp_min": min(doc["timestamp"]), "timestamp_max": max(doc["timestamp"])})
        elif dtype == 'event':
            coll_name = '%s_%s' % (token, self.event_coll_suffix)
            coll = self.db[coll_name]
            inserted = coll.insert_one({self.event_coll_suffix: doc[self.event_coll_suffix], "timestamp_min": min(doc["timestamp"]), "timestamp_max": max(doc["timestamp"])})
        else:
            raise ValueError('Invalid d-stream type.')

        if inserted.acknowledged is True:
            return inserted.inserted_id
        else:
            raise ValueError('Insert failed.')

    def get_by_id(self, doc_id, dtype, token=None):
        if dtype == 'template':
            doc = self.temp_collection.find_one({"_id": doc_id})
        elif dtype == 'derived':
            coll_name = '%s_%s' % (token, self.derived_coll_suffix)
            coll = self.db[coll_name]
            doc = coll.find_one({"_id": doc_id})
        elif dtype == 'event':
            coll_name = '%s_%s' % (token, self.event_coll_suffix)
            coll = self.db[coll_name]
            doc = coll.find_one({"_id": doc_id})
        else:
            raise ValueError('Invalid d-stream type')

        if doc is not None:
            return doc

        else:
            raise ValueError('Document does not exist.')

    def get_all_coll(self, dtype, token):
        if dtype == 'derived':
            coll_name = '%s_%s' % (token, self.derived_coll_suffix)
            coll = self.db[coll_name]
        elif dtype == 'event':
            coll_name = '%s_%s' % (token, self.event_coll_suffix)
            coll = self.db[coll_name]
        else:
            raise ValueError('Invalid stream type')

        docs = list(coll.find())

        if docs is not None:
            return docs
        else:
            raise ValueError("Collection is empty or does not exist.")
