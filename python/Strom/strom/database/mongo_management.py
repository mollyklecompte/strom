"""Mongodb Manager Class

This class contains methods for inserting and reading from Mongodb
"""

from pymongo import MongoClient

__version__  = "0.1"
__author__ = "Molly <molly@tura.io>"

# temporary config
config = {'mongo_host' : '172.17.0.2', 'mongo_port' : 27017, 'template_coll' : 'template_directory', 'db' : 'strom'}


class MongoManager(object):

    def __init__(self):
        self.client = MongoClient(config['mongo_host'], config['mongo_port'])
        self.db = self.client[config['db']]
        self.temp_coll_name = config['template_coll']
        self.temp_collection = self.db[self.temp_coll_name]

    def _insert_template(self, dstream):
        template = dstream
        template['_id'] = template['stream_token']
        inserted = self.temp_collection.insert_one(template)

        if inserted.acknowledged is True:
            return inserted.inserted_id
        else:
            raise ValueError('Template insert failed.')

    def _get_template(self, token):
        template = self.temp_collection.find_one({"_id": token})

        if template is not None:
            return template

        else:
            raise ValueError('Template with id ' + token + ' does not exist.')


