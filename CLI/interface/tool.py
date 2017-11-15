""" CLI tool for comms b/w client and API.
ANSI coloring is based on this methodology:
Creation = Magenta, Modification = Cyan, Response = Yellow, Error = Red """
import click
import requests
from termcolor import cprint
from pyfiglet import figlet_format

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

@click.group()
def dstream():
    """ Command group for all DStream methods. """
    pass

@click.command()
def init():
    """ Initialize DStream object. Returns stream_token. """
    click.echo(click.style("Creating DStream.....", fg='magenta'))
    try:
        ret = requests.get('http://127.0.0.1:5000/api/dstream/init')
    except:
        click.echo(click.style("Connection Refused!...", fg='red', reverse=True))
    else:
        click.echo(click.style(ret.status_code, fg='yellow'))
        click.echo(click.style(ret.text, fg='yellow'))

dstream.add_command(init)
