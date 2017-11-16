"""
B-stream class. Created .
"""

from dstream import Dstream

__version__ = "0.1"
__author__ = "Molly <molly@tura.io>"


class Bstream(Dstream):
    def __init__(self, template, dstreams):
        super().__init__()
        self.template = template
        self.dstreams = dstreams

    def _set_from_temp(self):
        self["stream_name"] = self.template["stream_name"]
        self["version"] = self.template["version"]
        self["stream_token"] = self.template["stream_token"]
        self["sources"] = self.template["sources"]
        self["storage_rules"] = self.template["storage_rules"]
        self["ingest_rules"] = self.template["ingest_rules"]
        self["engine_rules"] = self.template["engine_rules"]
        self["fields"] = self.template["fields"]
        self["user_ids"] = self.template["user_ids"]
        self["tags"] = self.template["tags"]
        self["foreign_keys"] = self.template["foreign_keys"]
        self["filters"] = self.template["filters"]
        self["dparam_rules"] = self.template["dparam_rules"]
        self["event_rules"] = self.template["event_rules"]

    def _aggregate_measures(self):
        all_measures = [s["measures"] for s in self.dstreams]
        self["measures"] = {m: {'val': [i[m]['val'] for i in all_measures], 'dtype': v['dtype']} for m, v in self.template.measures.items()}

    def _aggregate_ts(self):
        self["timestamps"] = [s["timestamp"] for s in self.dstreams]