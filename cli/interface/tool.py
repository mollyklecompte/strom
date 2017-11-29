""" CLI tool for communications b/w client and API. """
import click
import requests
import json
import os
import datetime
try:
    from pyfiglet import Figlet
except:
    click.secho("Pyfiglet failed to import! You don't get any pretty fonts.", fg='red', reverse=True)

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

def _convert_to_utc(date_string):
    """ Expected input: YYYY-MM-DD-HH:MM:SS """
    big_time_tmp = date_string.split("-")
    year = int(big_time_tmp[0])
    month = int(big_time_tmp[1])
    day = int(big_time_tmp[2])
    time_arr = big_time_tmp[3].split(":")
    hours = int(time_arr[0])
    minutes = int(time_arr[1])
    seconds = int(time_arr[2])
    dt = datetime.datetime(year, month, day, hours, minutes, seconds)
    return dt.timestamp()

def _api_POST(function, data_dict):
    """ Takes name of endpoint and dict of data to post. """
    try:
        ret = requests.post(url + "/api/{}".format(function), data=data_dict)
    except:
        click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
    else:
        click.secho(str(ret.status_code), fg='yellow')
        click.secho(ret.text, fg='yellow')
        return [ret.status_code, ret.text]

def _api_GET(function, param, value, token):
    """ Takes name of endpoint, time/range and its value, as well as token. """
    try:
        ret = requests.get(url + "/api/get/{}?".format(function) + "{}={}".format(param, value) + "&token={}".format(token))
    except:
        click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
    else:
        click.secho(str(ret.status_code), fg='yellow')
        click.secho(ret.text, fg='yellow')
        return [ret.status_code, ret.text]

def _collect_token(cert):
    """ Function to load json-formatted input and return stream_token, if found. """
    try:
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        try:
            token = json_cert["stream_token"]
            if token is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='yellow', reverse=True)
        else:
            click.secho("Found stream_token: " + token + '\n', fg='white')
            return token

def _check_options(function, time, utc, a, token):
    """ Function to check options given for GET methods before send. """
    if a:
        result = _api_GET("{}".format(function), "range", "ALL", token)
    elif utc:
        if len(utc) == 1:
            result = _api_GET("{}".format(function), "time", utc[0], token)
        elif len(utc) == 2:
            result = _api_GET("{}".format(function), "range", utc, token)
        else:
            click.secho("Too many arguments given!({})...".format(len(utc)), fg='yellow', reverse=True)
    elif time:
        if len(time) == 1:
            utime = _convert_to_utc(time[0])
            result = _api_GET("{}".format(function), "time", utime, token)
        elif len(time) == 2:
            utime_zero = _convert_to_utc(time[0])
            utime_one = _convert_to_utc(time[1])
            utime = [utime_zero, utime_one]
            result = _api_GET("{}".format(function), "range", utime, token)
        else:
            click.secho("Too many arguments given!({})...".format(len(time)), fg='yellow', reverse=True)
    else:
        click.secho("No options given, try '--all'...", fg='white')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@click.group()
@click.option('--version', '--v', 'version', is_flag=True, callback=_print_ver, expose_value=False, is_eager=True, help="Current version")
def dstream():
    """ Entry-point. Parent command for all DStream methods. """
    pass

@click.command()
def welcome():
    """ Usage instructions for first time users. """
    f = Figlet(font='slant')
    click.secho(f.renderText("Strom-C.L.I.\n"), fg='cyan')
    click.secho("USAGE INSTRUCTIONS:\n", fg='cyan', underline=True)
    click.secho("1. dstream define -template [template filepath]\n", fg='green')
    click.secho("2. dstream load -filepath [data filepath] -token [template token file]\n", fg='green')
    click.secho("3. dstream events --all -token [template token file]\n", fg='green')
    click.pause()

@click.command()
@click.option('-template', '-t', 'template', prompt=True, type=click.File('r'), help="Template file with required and custom fields")
@click.option('--yes', is_flag=True, callback=_abort_if_false, expose_value=False, prompt="\nInitialize new DStream with this template?", help="Bypass confirmation prompt")
def define(template):
    """ Upload template file for DStream. """
    template_data = template.read()
    click.secho("\nSending template file...", fg='white')
    #Try send template to server, if success...collect stream_token
    result = _api_POST("define", {'template':template_data})
    if result[0] == 202:
        token = result[1]
    else:
        click.secho("\nServer Error!...\n", fg='red', reverse=True)
    #Try load template as json and set stream_token field, if success...store tokenized template in new file
    try:
        json_template = json.loads(template_data)
        json_template['stream_token'] = token
        template_filename = os.path.basename(template.name) # NOTE: TEMP, REFACTOR OUT OF TRY
        path_list = template_filename.split('.')
        template_name = path_list[0]
        template_ext = path_list[1]
        print("Found File Extension: .{}".format(template_ext))  # NOTE: TEMP
    except:
        click.secho("\nProblem parsing template file!...\n", fg='red', reverse=True)
    else:
        click.secho("\nTemplate has been tokenized with...{}".format(json_template['stream_token']), fg='white')
        template_file = open("{}_token.txt".format(template_name), "w")
        template_file.write(json.dumps(json_template))
        template_file.close()
        click.secho("New template stored locally as '{}_token.txt'.\n".format(template_name))

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
        tk = _collect_token(cert)
        click.secho("\nSending source for this DStream...\n", fg='white')
        #Try posting data to server, if success...return status_code
        result = _api_POST("add-source", {'source':source, 'topic':kafka_topic, 'token':tk})

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
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
    else:
        #Try collect stream_token, if success...pass
        tk = _collect_token(cert)
        #Try set stream_token fields to collected token, if success...pass
        try:
            with click.progressbar(json_data) as bar:
                for obj in bar:
                    obj['stream_token'] = tk
        except:
            click.secho("Data file not correctly formatted!...\n", fg='red', reverse=True)
        else:
            click.secho("\nSending data...", fg='white')
            #Try send data with token to server, if success...return status_code
            result = _api_POST("load", {'data':json.dumps(json_data)})

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def raw(time, utc, a, tk):
    """
    \b
     Collect all raw data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    token = _collect_token(cert)
    _check_options("raw", time, utc, a, token)

@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def filtered(time, utc, a, tk):
    """
    \b
     Collect all filtered data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    token = _collect_token(cert)
    _check_options("filtered", time, utc, a, token)

@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def derived_params(time, utc, a, tk):
    """
    \b
     Collect all derived parameters for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    token = _collect_token(cert)
    _check_options("derived_params", time, utc, a, token)

@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', prompt=True, type=click.File('r'), help="Tokenized template file for verification")
def events(time, utc, a, tk):
    """
    \b
     Collect all event data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    cert = tk.read()
    token = _collect_token(cert)
    _check_options("events", time, utc, a, token)

# d-stream group
dstream.add_command(welcome)
dstream.add_command(define)
dstream.add_command(add_source)
dstream.add_command(load)
#
# dstream.add_command(raw)  #NOTE: TEMP
# dstream.add_command(filtered)
# dstream.add_command(derived_params)
dstream.add_command(events)
