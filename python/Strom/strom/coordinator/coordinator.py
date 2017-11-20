"""
Coordinator class

"""

from Strom.strom.dstream.bstream import BStream
from Strom.strom.database.mongo_management import MongoManager

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"

class Coordinator(object):
    def __init__(self):
        self.mongo = MongoManager()

    def _store_json(self, data, data_type):
        insert_id = self.mongo.insert(data, data_type)

        return insert_id

    def _store_template_metadata(self, mongo_id, stream_token, stream_name, version):
        """
        stores template metadata in mariadb by calling mariadb insert_row method
        :param mongo_id: the template's mongodb ib
        :param stream_token: stream token
        :param stream_name: stream name
        :param version: stream version
        :return: sql id for metadata
        """
        pass

    def _store_raw(self, data_list):
        ids = []
        fake_id = 1
        for i in data_list:
            ids.append(fake_id)
            fake_id += 1

        print("Inserted rows: %s-%s") % (ids[0], ids[-1])
        return ids

    def _store_filtered(self, bstream):
        ids = bstream.ids
        table = bstream['stream_token']
        zippies = {}
        for m,v in bstream['filter_measures'].items():
            z = zip(m['val'], ids)
            zippies[m] = list(z)

        for m,v in zippies.items():
            for i in v:
                print("INSERT %s INTO %s WHERE id= %s") % (i[0], table.m, i[1])

        print("Filtered measures added")


    def _list_to_bstream(self, template, dstreams, ids):
        bstream = BStream(template, dstreams, ids)

        return bstream

    def _apply_filters(self, bstream):
        return bstream

    def _apply_derived_params(self, bstream):
        return bstream

    def _apply_events(self, bstream):
        return bstream

    def _retrieve_current_template(self, token):
        """
        calls mariadb method to query metadata table by stream token, return mongodb id for highest version template. then, calls mongo_management method to retrieve template document by id, returning the template doc
        :param temp_id: template's unique id in mongodb
        :return: template json
        """
        temp_id = "this will be a mariadb method for query"
        template = self.mongo.get_by_id(temp_id, 'template')

        return template

    def process_template(self, temp_dstream):
        token = temp_dstream["stream_token"]
        name = temp_dstream["stream_name"]
        version = temp_dstream["version"]
        mongo_id = self._store_json(temp_dstream, 'template')
        inserted = self._store_template_metadata(mongo_id, token, name, version)

        print("Template inserted into template table. Id: %s") % inserted

    def process_data_sync(self, dstream_list, token):
        # store raw dstream data, return list of ids
        stream_ids = self._store_raw(dstream_list)

        # retrieve most recent versioned dstream template
        template = self._retrieve_current_template(token)

        # create bstream for dstream list
        bstream = self._list_to_bstream(template, dstream_list, stream_ids)

        # filter bstream data
        filtered_bstream = self._apply_filters(bstream)

        # store filtered dstream data
        self._store_filtered(filtered_bstream)

        # apply derived param transforms
        derived_bstream = self._apply_derived_params(filtered_bstream)

        # store derived params
        self._store_json(derived_bstream, 'derived')

        # apply derived param transforms
        events = self._apply_events(derived_bstream)

        # store events
        self._store_json(events, 'event')

        print("whoop WHOOOOP")








