"""
Coordinator Module

Stores Dstream templates and template metadata via MongoDB & MariaDB interface classes.
Handles the main data transformation & (asynchronous) storage process in
`process_data_async` method.
Creates Bstream objects from Dstream lists by calling Bstream class aggregate method.
Applies transformations to Bstreams by calling relevant Bstream class methods.
Handles Bstream data storage via StorageThread classes at appropriate steps.
"""
import json
import pickle
import time
import uuid

import pandas as pd
import requests

from strom.dstream.bstream import BStream
from strom.utils.configer import configer as config
from strom.utils.logger.logger import logger

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Coordinator(object):
    def __init__(self):
        self.threads = []

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
        # PUT STUFF HERE
        for key, val in temp_dstream.items():
            if type(val) == uuid.UUID:
                temp_dstream[key] = str(val)
        return self.return_template_df(temp_dstream)

        # self._post_template(temp_dstream)

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
        template = dstream_list[0]

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
        self._post_dataframe(bstream["stream_token"], bstream["measures"])

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
        logger.fatal("Parsing event")
        context_data = bstream["measures"]
        parsed_events = [
            {
                "event": "{}".format(event_name.replace(" ", "")),
            "data": single_row.to_json()
            }
            for event_name, event_df in bstream[config['event_coll_suf']].items()
            for single_ind, single_row in context_data.join(event_df['event_name'], how="right").iterrows()
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
        logger.fatal(event_data)
        r = requests.post(endpoint, json=event_data)

        return {'request_status': r.status_code}

    @staticmethod
    def _post_template(template):
        """
        Sends post request containing event data to API
        :param event_data: event data (individual event)
        :type event_data: dict
        :return: request status
        :rtype: string
        """

        logger.debug(template)


        temp_pd = pd.DataFrame.from_dict([{
            'stream_token': template['stream_token'],
            'template_id': template['template_id'],
            'stream_name': template['stream_name'],
            'version': template['version'],
            'user_description': template['user_description'],
            'template': json.dumps(template),
            'index':0}])

        logger.debug("made DataFrame from template")

        endpoint = 'http://{}:{}/template_storage'.format(config['server_host'],
                                                 config['server_port'])
        logger.debug("posting to "+endpoint)
        r = requests.post(endpoint, data=pickle.dumps(temp_pd))
        logger.debug("Finished post")
        return 'request status: ' + str(r.status_code)

    @staticmethod
    def return_template_df(template):
        temp_pd = pd.DataFrame.from_dict([{
            'stream_token': template['stream_token'],
            'template_id': template['template_id'],
            'stream_name': template['stream_name'],
            'version': template['version'],
            'user_description': template['user_description'],
            'template': json.dumps(template),
            'index': 0}])

        return temp_pd

    @staticmethod
    def _post_dataframe(stream_token, dataframe):
        """
        Sends post request containing event data to API
        :param event_data: event data (individual event)
        :type event_data: dict
        :return: request status
        :rtype: string
        """
        endpoint = 'http://{}:{}/data_storage'.format(config['server_host'],
                                                   config['server_port'])
        logger.debug(dataframe)
        r = requests.post(endpoint, data=pickle.dumps((stream_token, dataframe)))

        return 'request status: ' + str(r.status_code)
