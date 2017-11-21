""" Flask-API server for communications b/w CLI and services. """
from flask import Flask
from flask_restful import reqparse
from Strom.strom.dstream.dstream import DStream

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

app = Flask(__name__.split('.')[0])

arguments = ['template', 'data', 'source', 'topic', 'token']

parser = reqparse.RequestParser()
for word in arguments:
    parser.add_argument(word)

ds = DStream()# NOTE: TEMP


def define():
    """ Route to collect template for DStream init and return stream_token. """
    args = parser.parse_args()
    template = args['template']
    print(template)
    return str(ds['stream_token']), 200

def add_source():
    """ Route to collect data source and set in DStream field """
    args = parser.parse_args()
    if args['topic'] is not None:
        topic = args['topic']
        print(topic)
    source = args['source']
    token = args['token']
    print(source)
    print(token)
    return 'Success.', 202

def load():
    """ Route to collect tokenized data. """
    args = parser.parse_args()
    data = args['data']
    print(data)
    return 'Success.', 202

app.add_url_rule('/api/define', 'define', define, methods=['POST'])
app.add_url_rule('/api/load', 'load', load, methods=['POST'])
app.add_url_rule('/api/add-source', 'add_source', add_source, methods=['POST'])

def start():
    """ Entry-point """
    app.run()
if __name__ == '__main__':
    start()
