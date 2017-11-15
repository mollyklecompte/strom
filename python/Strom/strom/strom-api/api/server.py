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

ds = DStream()

def init():
    """ Route for initializing DStream object. """
    return "Welcome to Strom API. DStream token: {}".format(ds['stream_token'])

def define():
    """ Route for defining DStream. """
    if request.method == 'POST':
        args = parser.parse_args()
        print(args)
        return ''
    else:
        return '', 400

app.add_url_rule('/init', 'init', init, methods=['GET'])
app.add_url_rule('/define', 'define', define, methods=['GET', 'POST'])

def start():
    """ Entrypoint """
    app.run()
if __name__ == '__main__':
    start()
