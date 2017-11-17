""" CLI tool for comms b/w client and API. """
import click
import requests
import json

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'
url = 'http://127.0.0.1:5000'

def prnt_ver(ctx, param, value):
    if not value or ctx.resilient_parsing:
        return
    click.secho(__version__, fg='yellow')
    ctx.exit()

@click.group()
@click.option('--version', '--v', 'version', is_flag=True, callback=prnt_ver, expose_value=False, is_eager=True, help="Current version")
def dstream():
    """ Entrypoint. Command group for all DStream methods. """
    pass

@click.command()
@click.option('-template', '-t', 'template', prompt=True, type=click.File('r'), help="Template file to initialize DStream")
def define(template):
    """  """
    data = template.read()
    click.secho("\nSending template file...", fg='white')
    try:
        ret = requests.post(url + "/api/define", data={'template':data})
    except:
        click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
    else:
        click.secho(str(ret.status_code), fg='yellow')
        click.secho(ret.text, fg='yellow')
    token = ret.text
    try:
        json_data = json.loads(data)
        json_data['stream_token'] = token
    except:
        click.secho("\nProblem parsing template file!...\n", fg='red', reverse=True)
    else:
        click.secho("\nTemplate has been tokenized with...{}".format(json_data['stream_token']), fg='white')
        template_file = open("demo_data/tokenized_template.txt", "w")
        template_file.write(json.dumps(json_data))
        template_file.close()
        click.secho("New template stored as 'tokenized_template.txt'.\n")

@click.command()
@click.option('-filepath', '-f', 'filepath', prompt=True, type=click.Path(exists=True), help="Filepath of data file to upload")
@click.option('-token', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def load(filepath, token):
    """  """
    click.secho("\nTokenizing data fields of {} with...".format(click.format_filename(filepath)), fg='white')
    cert = token.read()
    try:
        json_data = json.load(open(filepath))
    except:
        click.secho("There is an error accessing that filepath!...\n", fg='red', reverse=True)
    else:
        json_cert = json.loads(cert)
        try:
            token = json_cert['stream_token']
            if token is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template...\n", fg='red', reverse=True)
        else:
            click.secho(token + '\n', fg='white')
            try:
                with click.progressbar(json_data) as bar:
                    for obj in bar:
                        obj['stream_token'] = token
            except:
                click.secho("Data file not correctly formatted!...\n", fg='red', reverse=True)
            else:
                click.secho("\nSending tokenized data...", fg='white')
                try:
                    ret = requests.post(url + '/api/load', data={'data':json.dumps(json_data)})
                except:
                    click.secho("Connection Refused!...\n", fg='red', reverse=True)
                else:
                    click.secho(str(ret.status_code), fg='yellow')
                    click.secho(ret.text + '\n', fg='yellow')

# d-stream group
dstream.add_command(define)
dstream.add_command(load)
