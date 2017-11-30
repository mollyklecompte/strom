""" Flask-API server for communications b/w CLI and services. """
import json
from flask import Flask, request
from flask_restful import reqparse
from Strom.strom.dstream.dstream import DStream
from Strom.strom.coordinator.coordinator import Coordinator

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

app = Flask(__name__.split('.')[0])

arguments = ['template', 'data', 'source', 'topic', 'token']

parser = reqparse.RequestParser()
for word in arguments:
    parser.add_argument(word)

cd = Coordinator() # NOTE: TEMP


def define():
    """ Route to collect template for DStream init and return stream_token. """
    args = parser.parse_args()
    template = args['template'] #   dstream template
    dstream_new = DStream()
    json_template = json.loads(template)
    dstream_new.load_from_json(json_template)
    cd.process_template(dstream_new)
    return str(dstream_new['stream_token']), 202

def add_source():
    """ Route to collect data source and set in DStream field """
    args = parser.parse_args()
    if args['topic'] is not None:
        topic = args['topic']   #   kafka topic
        print(topic)
    source = args['source'] #   file/kafka
    token = args['token']   #   stream_token
    print(source)
    print(token)
    return 'Success.', 202

def load():
    """ Route to collect tokenized data. """
    args = parser.parse_args()
    data = args['data'] #   data with token
    json_data = json.loads(data)
    token = json_data[0]['stream_token']
    print(token)
    cd.process_data_sync(json_data, token)
    return 'Success.', 202

def get(this):
    """ Route for returning data, specified by endpoint & URL params. """
    time_range = request.args.get('range', '')
    time = request.args.get('time', '')
    token = request.args.get('token', '')
    print(this) #   endpoint: raw, filtered, derived_params, events
    if time_range:
        if time_range == 'ALL':
            result = cd.get_events(token)
    return ("\n" + str(result) + "\n"), 200

#   POST
app.add_url_rule('/api/define', 'define', define, methods=['POST'])
app.add_url_rule('/api/add-source', 'add_source', add_source, methods=['POST'])
app.add_url_rule('/api/load', 'load', load, methods=['POST'])
#   GET
app.add_url_rule('/api/get/<this>', 'get', get, methods=['GET'])

def start():
    """ Entry-point """
    app.run()
if __name__ == '__main__':
    start()