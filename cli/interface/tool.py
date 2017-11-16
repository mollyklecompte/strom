""" CLI tool for comms b/w client and API.
ANSI coloring is based on this methodology:
Creation = Magenta, Modification = Cyan, Response = Yellow, Error = Red """
import click
import requests
from termcolor import cprint
from pyfiglet import figlet_format

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

def prnt_ver(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.echo(click.style(__version__, fg='yellow'))
    ctx.exit()

@click.group()
@click.option('--version', '--v', 'version', is_flag=True, callback=prnt_ver, expose_value=False, is_eager=True, help="Current version")
def dstream():
    """ Entrypoint. Command group for all DStream methods. """
    pass

@click.command()
def welcome():
    """ Usage instructions for first-time users. """
    cprint(figlet_format("Strom CLI", font='small'), 'yellow')
    click.echo(click.style("Initialize a DStream object using 'dstream init'...", fg='magenta'))
    click.echo(click.style("Save your dstream_token in a safe place!", fg='magenta', underline=True))
    click.echo(click.style("Define source of data(kafka stream or file upload) with 'dstream define --source'...", fg='cyan'))
    click.echo(click.style("If source is a file: specify file with '--file'.", fg='cyan'))
    click.echo(click.style("If source is a kafka stream: specify topic with '--kafka-topic'.", fg='cyan'))

@click.command()
def init():
    """ Initialize DStream object. Returns stream_token. """
    click.echo(click.style("Creating DStream.....\n", fg='magenta'))
    try:
        ret = requests.get('http://127.0.0.1:5000/init')
    except:
        click.echo(click.style("Connection Refused!...\n", fg='red', reverse=True))
    else:
        click.echo(click.style(str(ret.status_code), fg='yellow'))
        click.echo(click.style(ret.text, fg='yellow'))

@click.command()
@click.option('--source', prompt=True, type=click.Choice(['kafka', 'file']), help="Specify source of data")
@click.option('--file', 'f', type=click.File('r'), help="Template file to upload")
@click.option('--topic', default=None, help="Specify kafka topic")
def define(source, f, topic):
    """ Define source of DStream. """
    if source == 'kafka':
        click.echo(click.style("Adding Kafka topic...\n", fg='cyan'))
        try:
            ret = requests.post('http://127.0.0.1:5000/define', data={'type':'kafka', 'content':topic})
        except:
            click.echo(click.style("Connection Refused!...\n", fg='red', reverse=True))
        else:
            click.echo(click.style(str(ret.status_code), fg='yellow'))
            click.echo(click.style(ret.text, fg='yellow'))

    else:
        click.echo(click.style("Adding template...\n", fg='cyan'))
        data = f.read()
        try:
            ret = requests.post('http://127.0.0.1:5000/define', data={'type':'file', 'content':data})
        except:
            click.echo(click.style("Connection Refused!...\n", fg='red', reverse=True))
        else:
            click.echo(click.style(str(ret.status_code), fg='yellow'))
            click.echo(click.style(ret.text, fg='yellow'))


# d-stream group
dstream.add_command(welcome)
dstream.add_command(init)
dstream.add_command(define)
