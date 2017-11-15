""" Flask-API server for comms b/w CLI and services. """
import json
from flask import Flask, request
from flask_restful import reqparse

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

app = Flask(__name__.split('.')[0])
parser = reqparse.RequestParser()

parser.add_argument('source')#  TEMP

def init():
    """ Route for initializing DStream object. """
    return "Welcome to Strom API.", 200

def define():
    """ Route for defining DStream. """
    return '', 404# NOTE TEMP

app.add_url_rule('/', 'init', init, methods=['GET'])
app.add_url_rule('/define', 'define', methods=['GET', 'POST'])

def start():
    app.run()
if __name__ == '__main__':
    start()
