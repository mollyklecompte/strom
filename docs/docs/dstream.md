# DStream
DStreams are the basic unit of data used by Strom.

Internally, they are a dict subclass with the following keys:

- [version](version)
- stream_token
- sources
- storage_rules
- ingest_rules
- engine_rules
- timestamp
- measures
- fields
- user_ids
- tags
- foreign_keys
- filters
- dparam_rules
- event_rules

<a name="version">version: The version number of the DStream</a>