# DStream
DStreams are the basic unit of data used by Strom.
They take on two roles. When a new data source is registered, a DStream template is created. This
template defines the expected data format as well as establishing rules for the data storege, etc,
but does not include any actual data measurements.
This template is then used as the format for the second role, sending data. The actual data
measurements are inserted into the template and sent to the Strom server.

Structurally DStreams are a dict subclass with a set of expected keys. S

- verison
  - The version number for the dstream.
  - Set when a new DStream is created and incremented when the dstream is updated
  - type: int
- stream_token
  - internally generated UUID to identify this DStream. Created when a new DStream is initialized
  - type: python UUID
- sources
  - list of data sources. Currently unused.
- storage_rules:
  - Rules for when the DStream data is stored.
  - See stream_rules.py for expected format
  - type: dict
- ingest_rules
  - Rules for how to handle the data as it is ingested by our engine.
  - Currently unused but plan to use it for NaN handling, etc.
  - See stream_rules.py for expected format
  - type: dict
- engine_rules
  - Rules that govern how the Engine class process the data
  - See stream_rules.py for expected format
  - type: dict
- filters
  - Rules for creating filtered measures from the raw DStream measures
  - See stream_rules.py for expected format
  - type: list of dict
- dparam_rules
  - Rules for creating derived measures from one or more raw DStream measures
  - See stream_rules.py for expected format
  - type: list of dict
- event_rules
  - Rules defining the events to be detected
  - See stream_rules.py for expected format
  - type: list of dict

- timestamp
- measures
- fields
- user_ids
- tags
- foreign_keys
