""" Flask-API server for comms b/w CLI and services. """
import json
from flask import Flask, request
from flask_restful import reqparse
from Strom.strom.dstream.dstream import DStream

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

app = Flask(__name__.split('.')[0])

parser = reqparse.RequestParser()
parser.add_argument('type')
parser.add_argument('source')

ds = DStream()# NOTE: TEMP

def init():
    """ Route for initializing DStream object. """
    return "Welcome to Strom API. DStream token: {}".format(ds['stream_token'])

def define():
    """ Route for defining DStream. """
    args = parser.parse_args()
    if args['source']:
        typ = args['type']
        src = args['source']
        print(typ)# NOTE: TEMP
        print(src)# NOTE: TEMP
        if typ == 'file':
            ds.load_from_json(src)
        return '', 202
    else:
        return 'Missing Data...', 400

def modify():
    """ Route for adding or modifying DStream fields. """
    pass

app.add_url_rule('/init', 'init', init, methods=['GET'])
app.add_url_rule('/define', 'define', define, methods=['POST'])
app.add_url_rule('/modify', 'modify', modify, methods=['GET', 'POST'])

def start():
    """ Entrypoint """
    app.run()
if __name__ == '__main__':
    start()
