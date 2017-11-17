""" Flask-API server for comms b/w CLI and services. """
import json
from flask import Flask, request
from flask_restful import reqparse
from Strom.strom.dstream.dstream import DStream

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

app = Flask(__name__.split('.')[0])

parser = reqparse.RequestParser()
parser.add_argument('template')
parser.add_argument('data')

ds = DStream()# NOTE: TEMP

def define():
    """ Route to collect template for DStream init and return stream_token. """
    args = parser.parse_args()
    template = args['template']
    print(template)
    return str(ds['stream_token']), 202

def load():
    """ Route to collect tokenized data. """
    args = parser.parse_args()
    data = args['data']
    print(data)
    return 'Success.', 202

app.add_url_rule('/api/define', 'define', define, methods=['POST'])
app.add_url_rule('/api/load', 'load', load, methods=['POST'])

def start():
    """ Entrypoint """
    app.run()
if __name__ == '__main__':
    start()
