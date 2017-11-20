""" CLI tool for communications b/w client and API. """
import click
import requests
import json

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'

url = 'http://127.0.0.1:5000'

def _print_ver(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.secho(__version__, fg='yellow')
    ctx.exit()

def _abort_if_false(ctx, param, value):
    if not value:
        ctx.abort()

@click.group()
@click.option('--version', '--v', 'version', is_flag=True, callback=_print_ver, expose_value=False, is_eager=True, help="Current version")
def dstream():
    """ Entry-point. Parent command for all DStream methods. """
    pass

@click.command()
def welcome():
    """ Usage instructions for first time users. """
    click.secho("\nJust Do It.\n", fg='magenta', bold=True)
    click.pause()

@click.command()
@click.argument('path')
def locate(path):
    """ Tool for opening specified files. """
    click.launch(path, locate=True)

@click.command()
@click.option('-template', '-t', 'template', prompt=True, type=click.File('r'), help="Template file with required and custom fields")
@click.option('--yes', is_flag=True, callback=_abort_if_false, expose_value=False, prompt="\nInitialize new DStream with this template?", help="Bypass confirmation prompt")
def define(template):
    """ Upload template file for DStream. """
    template_data = template.read()
    click.secho("\nSending template file...", fg='white')
    #Try send template to server, if success...collect stream_token
    try:
        ret = requests.post(url + "/api/define", data={'template':template_data})
    except:
        click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
    else:
        click.secho(str(ret.status_code), fg='yellow')
        click.secho(ret.text, fg='yellow')
        token = ret.text
        #Try load template as json and set stream_token field, if success...store tokenized template in new file
        try:
            json_template = json.loads(template_data)
            json_template['stream_token'] = token
        except:
            click.secho("\nProblem parsing template file!...\n", fg='red', reverse=True)
        else:
            click.secho("\nTemplate has been tokenized with...{}".format(json_template['stream_token']), fg='white')
            template_file = open("tokenized_template.txt", "w")
            template_file.write(json.dumps(json_template))
            template_file.close()
            click.secho("New template stored locally as 'tokenized_template.txt'.\n")

@click.command()
@click.option('-source', '-s', 'source', prompt=True, type=click.Choice(['kafka', 'file']), help="Specify source of data")
@click.option('--kafka-topic', default=None, help="If source is kafka, specify topic")
@click.option('-token', '-tk', 'token', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def add_source(source, kafka_topic, token):
    """ Declare source of data: file upload or kafka stream. """
    #Check if topic was supplied when source is kafka
    if source == 'kafka' and kafka_topic == None:
        click.secho("No topic specified, please re-run command.", fg='yellow', reverse=True)
    else:
        cert = token.read()
        #Try loading template as json and retrieving token, if success...pass
        try:
            json_cert = json.loads(cert)
            tk = json_cert['stream_token']
        except:
            click.secho("\nThere was an error parsing that file and/or the token was not found!...\n", fg='yellow', reverse=True)
        else:
            click.secho("\nFound stream_token: " + tk, fg='white')
            click.secho("\nSending source for this DStream...\n", fg='white')
            #Try posting data to server, if success...return status_code
            try:
                ret = requests.post(url + "/api/add-source", data={'source':source, 'topic':kafka_topic, 'token':tk})
            except:
                click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
            else:
                click.secho(str(ret.status_code), fg='yellow')
                click.secho(ret.text + '\n', fg='yellow')


@click.command()
@click.option('-filepath', '-f', 'filepath', prompt=True, type=click.Path(exists=True), help="File-path of data file to upload")
@click.option('-token', '-tk', 'token', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def load(filepath, token):
    """ Provide file-path of data to upload, along with tokenized_template for this DStream. """
    click.secho("\nTokenizing data fields of {}".format(click.format_filename(filepath)), fg='white')
    cert = token.read()
    #Try load client files as json, if success...pass
    try:
        json_data = json.load(open(filepath))
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        #Try collect stream_token, if success...pass
        try:
            tk = json_cert['stream_token']
            if tk is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='yellow', reverse=True)
        else:
            click.secho("Found stream_token: " + tk + '\n', fg='white')
            #Try set stream_token fields to collected token, if success...pass
            try:
                with click.progressbar(json_data) as bar:
                    for obj in bar:
                        obj['stream_token'] = tk
            except:
                click.secho("Data file not correctly formatted!...\n", fg='red', reverse=True)
            else:
                click.secho("\nSending tokenized data...", fg='white')
                #Try send data with token to server, if success...return status_code
                try:
                    ret = requests.post(url + "/api/load", data={'data':json.dumps(json_data)})
                except:
                    click.secho("Connection Refused!...\n", fg='red', reverse=True)
                else:
                    click.secho(str(ret.status_code), fg='yellow')
                    click.secho(ret.text + '\n', fg='yellow')

# d-stream group
dstream.add_command(locate)
dstream.add_command(welcome)
dstream.add_command(define)
dstream.add_command(add_source)
dstream.add_command(load)
