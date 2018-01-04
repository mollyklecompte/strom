"""
Coordinator class

"""
import time
from copy import deepcopy

import requests
from bson.objectid import ObjectId
from strom.database.maria_management import SQL_Connection
from strom.database.mongo_management import MongoManager
from strom.dstream.bstream import BStream
from strom.storage_thread.storage_thread import *
from strom.utils.configer import configer as config
from strom.utils.logger.logger import logger

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Coordinator(object):
    def __init__(self):
        self.mongo = MongoManager()
        self.maria = SQL_Connection()
        self.threads = []

    def _store_json(self, data, data_type):
        insert_id = self.mongo.insert(data, data_type)

        return insert_id

    def _store_raw(self, bstream):
        """
        Passes freshly aggregated bstream to maria manager for storage.
        :param bstream: a bstream
        :return:
        """
        rows_inserted = self.maria._insert_rows_into_stream_lookup_table(bstream)

        return rows_inserted

    def _store_raw_old(self, data_list):
        """
        Old version of store_raw where we iterated through dstreams and inserted them one by one. DO NOT USE
        :param data_list:
        :return:
        """
        ids = []

        for dstream in data_list:
            row_id = self.maria._insert_row_into_stream_lookup_table(dstream)
            ids.append(row_id)

        print("Inserted rows: %s-%s" % (ids[0], ids[-1]))
        return ids

    def _store_filtered(self, bstream):
        """
        Passes token, timestamp list, filtered measures to maria manager for storage
        :param bstream: b-stream dict with filtered data
        :return:
        """

        filtered_dict = {"stream_token": bstream["stream_token"], "timestamp": bstream["timestamp"], "filter_measures": bstream["filter_measures"]}
        rows_inserted = self.maria._insert_rows_into_stream_filtered_table(filtered_dict)

        return rows_inserted

    def _store_filtered_old_dumb(self, bstream):
        """
        This is when we foolishly updated rows in the stream table with the filtered data. Here for historical purposes only DO NOT USE
        :param bstream:
        :return:
        """
        ids = bstream.ids
        token = bstream['stream_token']
        zippies = {}
        for m,v in bstream['filter_measures'].items():
            z = zip(v['val'], ids)
            zippies[m] = list(z)

        for measure,val_id_pair_list in zippies.items():
            for i in val_id_pair_list:
                val = i[0]
                id = i[1]
                self.maria._insert_filtered_measure_into_stream_lookup_table(token, measure, str(val), id)

    def _list_to_bstream(self, template, dstreams):
        bstream = BStream(template, dstreams)
        bstream.aggregate

        return bstream

    def _retrieve_current_template(self, token):
        """
        calls mariadb method to query metadata table by stream token, return mongodb id for highest version template. then, calls mongo_management method to retrieve template document by id, returning the template doc
        :param temp_id: template's unique id in mongodb
        :return: template json
        """
        temp_id = ObjectId(self.maria._return_template_id_for_latest_version_of_stream(token))
        template = self.mongo.get_by_id(temp_id, 'template')

        return template

    def _retrieve_data_by_timestamp(self, dstream, time):
        """
        calls mariadb method to query stream lookup table and return the rows that fall within a time range, or just the matching row if the argument is a timestamp
        """
        # if time is a number
        if isinstance(time, int) or isinstance(time, float):
            return self.maria._retrieve_by_timestamp_range(dstream, time, time)
        #  if time is an array or tuple (time range)
        else:
            start = time[0]
            end = time[1]
            return self.maria._retrieve_by_timestamp_range(dstream, start, end)

    def _parse_events(self, bstream):
        parsed_events = [{"event": "{}_{}".format(event_name.replace(" ", ""), bstream['engine_rules']['kafka']), "data": single_event} for event_name, event_list in bstream[config['event_coll_suf']].items() for single_event in event_list]
        return parsed_events

    def _post_events(self, event_data):
        endpoint = 'http://{}:{}/new_event'.format(config['server_host'], config['server_port'])
        logger.debug(event_data)
        r = requests.post(endpoint, json=event_data)

        return 'request status: ' + str(r.status_code)

    def _post_parsed_events(self, bstream):
        events = self._parse_events(bstream)
        if len(events) >= 1:
            for event in events:
                self._post_events(event)
        else:
            logger.warning("No events detected")

    def process_template(self, temp_dstream):
        temp_dstream = deepcopy(temp_dstream)
        token = temp_dstream["stream_token"]
        name = temp_dstream["stream_name"]
        version = temp_dstream["version"]
        mongo_id = str(self._store_json(temp_dstream, 'template'))
        metadata_tabel_check =  self.maria._check_metadata_table_exists()
        if not metadata_tabel_check:
            self.maria._create_metadata_table()
        self.maria._insert_row_into_metadata_table(name, token, version, mongo_id)
        # Create stream lookup table for raw data
        self.maria._create_stream_lookup_table(temp_dstream)
        # Create stream lookup table for filtered data
        self.maria._create_stream_filtered_table(temp_dstream)

    def process_data_sync(self, dstream_list, token):


        st = time.time()

        # retrieve most recent versioned dstream template
        template = self._retrieve_current_template(token)

        # create bstream for dstream list
        bstream = self._list_to_bstream(template, dstream_list)

        # store raw measures from bstream
        self._store_raw(bstream)

        # filter bstream data
        bstream.apply_filters()

        # store filtered dstream data
        self._store_filtered(bstream)

        # apply derived param transforms
        bstream.apply_dparam_rules()

        # store derived params
        self._store_json(bstream, 'derived')

        # apply event transforms
        bstream.find_events()

        # store events
        self._store_json(bstream, 'event')
        print("whoop WHOOOOP", time.time() - st, len(bstream["timestamp"]))

    def process_data_async(self, dstream_list, token):

        logger.debug("process_data_async")
        st = time.time()
        # use StorageRules to validate
        # {"store_raw": True, "store_filtered": True, "store_derived": True}
        # retrieve most recent versioned dstream template
        template = self._retrieve_current_template(token)
        storage_rules = template['storage_rules']

        # create bstream for dstream list
        bstream = self._list_to_bstream(template, dstream_list)
        # thread store raw measures from bstream
        if storage_rules['store_raw'] :
            self.threads.append('raw_thread')
            raw_thread = StorageRawThread(bstream)
            raw_thread.start()
            logger.debug('store_raw thread started')

        # filter bstream data
        bstream.apply_filters()
        # thread store filtered dstream data
        if storage_rules['store_filtered'] :
            self.threads.append('filtered_thread')
            filtered_thread = StorageFilteredThread(bstream)
            filtered_thread.start()
            logger.debug('store_filtered thread started')

        # apply derived param transforms
        bstream.apply_dparam_rules()
        # thread store derived params
        if storage_rules['store_derived'] :
            self.threads.append('derived_thread')
            derived_thread = StorageJsonThread(bstream,'derived')
            derived_thread.start()
            logger.debug('store_json thread started')

        # apply event transforms
        bstream.find_events()
        # post events to server
        self._post_parsed_events(bstream)

        # thread store events
        event_thread = StorageJsonThread(bstream,'event')
        event_thread.start()
        print("whoop WHOOOOP", time.time() - st, len(bstream["timestamp"]))


    def get_events(self, token):
        return self.mongo.get_all_coll("event", token)
