from .fun_factory import build_template
from .route_wrappers import *


__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Strom(object):
    def __init__(self, server_url):
        self.url = server_url

    def create_strom(self, name, src_key, measure_rules, uids, filters, description="", storage_rules=None, ingest_rules=None, engine_rules=None, foreign_keys=None, tags=None, fields=None, source_mapping_list=None, dstream_mapping_list = None, puller=None, date_format=None):
        template = build_template(name, src_key, measure_rules, uids, filters, description, storage_rules, ingest_rules, engine_rules, foreign_keys, tags, fields, source_mapping_list, dstream_mapping_list, puller, date_format)
        r = post_template(self.url, template)

        if r[0] == 200:
            return r[1]
        else:
            return r