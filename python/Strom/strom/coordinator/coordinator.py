"""
Coordinator Module

Stores Dstream templates and template metadata via MongoDB & MariaDB interface classes.
Handles the main data transformation & (asynchronous) storage process in
`process_data_async` method.
Creates Bstream objects from Dstream lists by calling Bstream class aggregate method.
Applies transformations to Bstreams by calling relevant Bstream class methods.
Handles Bstream data storage via StorageThread classes at appropriate steps.
"""

import time
from copy import deepcopy

import requests

from strom.dstream.bstream import BStream
from strom.utils.configer import configer as config
from strom.utils.logger.logger import logger

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Coordinator(object):
    def __init__(self):
        self.threads = []

    def _retrieve_current_template(self, token):
        """
        Retrieves most recent/ highest versioned template for stream (by token).
        :param token: the stream token
        :type token: string
        :return: the current dstream template
        :rtype: dict
        """
        pass

    def _post_parsed_events(self, bstream):
        """Wrapper on `_parse_events` + `_post_events`- parses individual events & posts to API"""
        events = self._parse_events(bstream)
        if len(events) >= 1:
            for event in events:
                self._post_events(event)
        else:
            logger.warning("No events detected")


    def process_template(self, temp_dstream):
        """
        Wrapper method for template processing.
        Stores template metadata + template
        :param temp_dstream: dstream template
        :type temp_dstream: dict
        """
        temp_dstream = deepcopy(temp_dstream)
        # PUT STUFF HERE
        self._post_template(temp_dstream)

    def process_data(self, dstream_list, token):
        """
        Wrapper method for asynchronously processing data.
        :param dstream_list: list of dstreams with raw data
        :type dstream_list: list of dicts
        :param token: stream token
        :type token: string
        """
        logger.debug("process_data_async")
        st = time.time()

        # retrieve most recent versioned dstream template
        template = self._retrieve_current_template(token)

        # create bstream for dstream list
        bstream = self._list_to_bstream(template, dstream_list)

        # filter bstream data
        bstream.apply_filters()

        # apply derived param transforms
        bstream.apply_dparam_rules()

        # apply event transforms
        bstream.find_events()
        # post events to server
        self._post_parsed_events(bstream)
        self._post_dataframe(bstream["measures"])

        print("whoop WHOOOOP", time.time() - st, len(bstream["timestamp"]))


    @staticmethod
    def _list_to_bstream(template, dstreams):
        """
        Transforms list of dstreams into single bstream.
        :param template: dstream template
        :type template: dict
        :param dstreams: dstreams with raw data to be transformed
        :type dstreams: list of dicts
        :return: aggregated bstream
        :rtype: dict
        """
        bstream = BStream(template, dstreams)
        bstream.aggregate

        return bstream

    @staticmethod
    def _parse_events(bstream):
        """
        Parses fully transformed bstream into list of individual events
        :param bstream: bstream containing event data
        :type bstream: dict
        :return: list of individual events
        :rtype: list of dicts
        """
        context_data = bstream["measures"]
        parsed_events = [
            {
                "event": "{}_{}".format(event_name.replace(" ", ""),
                                        bstream['engine_rules']['kafka']),
            "data": single_row.to_json()
            }
            for event_name, event_df in bstream[config['event_coll_suf']].items()
            for single_ind, single_row in context_data.join(event_df.set_index("timestamp"), on="timestamp").iterrows()
        ]

        return parsed_events

    @staticmethod
    def _post_events(event_data):
        """
        Sends post request containing event data to API
        :param event_data: event data (individual event)
        :type event_data: dict
        :return: request status
        :rtype: string
        """
        endpoint = 'http://{}:{}/new_event'.format(config['server_host'],
                                                   config['server_port'])
        logger.debug(event_data)
        r = requests.post(endpoint, json=event_data)

        return 'request status: ' + str(r.status_code)

    @staticmethod
    def _post_template(template):
        """
        Sends post request containing event data to API
        :param event_data: event data (individual event)
        :type event_data: dict
        :return: request status
        :rtype: string
        """
        endpoint = 'http://{}:{}/storage'.format(config['server_host'],
                                                   config['server_port'])
        logger.debug(template)
        r = requests.post(endpoint, json=template)

        return 'request status: ' + str(r.status_code)

    @staticmethod
    def _post_dataframe(dataframe):
        """
        Sends post request containing event data to API
        :param event_data: event data (individual event)
        :type event_data: dict
        :return: request status
        :rtype: string
        """
        endpoint = 'http://{}:{}/storage'.format(config['server_host'],
                                                   config['server_port'])
        logger.debug(dataframe)
        r = requests.post(endpoint, json=dataframe.to_json())

        return 'request status: ' + str(r.status_code)
