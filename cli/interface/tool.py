""" CLI tool for communications b/w client and API."""
import click
import requests
import json
import os
import datetime
try:
    from pyfiglet import Figlet
except:
    pass

__version__ = '0.0.1'
__author__ = 'Adrian Agnic <adrian@tura.io>'


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

def _api_POST(config, function, data_dict):
    """ Takes name of endpoint and dict of data to post. """
    if config.verbose:
        click.secho("\nPOSTing to /{}".format(function), fg='white')
    try:
        ret = requests.post(config.url + "/api/{}".format(function), data=data_dict)
    except:
        click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
        if config.verbose:
            click.secho("Server connection was denied. Check your internet connections and try again. Otherwise contact support.", fg='cyan')
    else:
        click.secho(str(ret.status_code), fg='yellow')
        click.secho(ret.text, fg='yellow')
        return [ret.status_code, ret.text]

def _api_GET(config, function, param, value, token):
    """ Takes name of endpoint, time/range and its value, as well as token. """
    if config.verbose:
        click.secho("\nGETing {}={} from {} with {}".format(param, value, function, token), fg='white')
    try:
        ret = requests.get(config.url + "/api/get/{}?".format(function) + "{}={}".format(param, value) + "&token={}".format(token))
    except:
        click.secho("\nConnection Refused!...\n", fg='red', reverse=True)
        if config.verbose:
            click.secho("Server connection was denied. Check your internet connections and try again. Otherwise contact support.", fg='cyan')
    else:
        click.secho(str(ret.status_code), fg='yellow')
        click.secho(ret.text, fg='yellow')
        return [ret.status_code, ret.text]

def _collect_token(config, cert):
    """ Load json-formatted input and return stream_token, if found. """
    try:
        json_cert = json.loads(cert)
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
        if config.verbose:
            click.secho("The file you uploaded must be compatible with a JSON parser. Please revise and try again.", fg='cyan')
    else:
        if config.verbose:
            click.secho("Searching for token...", fg='white')
        try:
            token = json_cert["stream_token"]
            if token is None:
                raise ValueError
        except:
            click.secho("Token not found in provided template!...\n", fg='red', reverse=True)
            if config.verbose:
                click.secho("Make sure your using the template file generated from 'dstream define'!", fg='cyan')
        else:
            if config.verbose:
                click.secho("Found stream_token: " + token + '\n', fg='white')
            return token

def _check_options(config, function, time, utc, a, token):
    """ Check options given for GET methods before send. """
    if a:
        result = _api_GET(config, "{}".format(function), "range", "ALL", token)
    elif utc:
        if len(utc) == 1:
            result = _api_GET(config, "{}".format(function), "time", utc[0], token)
        elif len(utc) == 2:
            result = _api_GET(config, "{}".format(function), "range", utc, token)
        else:
            click.secho("Too many arguments given!({})...".format(len(utc)), fg='red', reverse=True)
    elif time:
        if len(time) == 1:
            utime = _convert_to_utc(time[0])
            result = _api_GET(config, "{}".format(function), "time", utime, token)
        elif len(time) == 2:
            utime_zero = _convert_to_utc(time[0])
            utime_one = _convert_to_utc(time[1])
            utime = [utime_zero, utime_one]
            result = _api_GET(config, "{}".format(function), "range", utime, token)
        else:
            click.secho("Too many arguments given!({})...".format(len(time)), fg='red', reverse=True)
    else:
        click.secho("No options given, try '--all'...", fg='cyan')

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
class CLIConfig(object):
    def __init__(self, verbose, store):
        self.verbose = verbose
        self.token = None
        self.url = 'http://127.0.0.1:5000'
        self.store = store

    def _set_token(self):
        f = open(".cli_token")
        data = f.read()
        if data is not None:
            self.token = data
        return self.token

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@click.group()
@click.option('-verbose', '-v', 'verbose', is_flag=True, help="Enables verbose mode")
@click.option('-store', '-S', 'store', is_flag=True, help="(SHORTCUT) Don't use if defining multiple templates. Allows omission of '-token'") # TODO change to callback
@click.option('--version', is_flag=True, callback=_print_ver, is_eager=True, expose_value=False, help="Current version")
@click.pass_context
def dstream(ctx, verbose, store):
    """ Entry-point. Parent command for all DStream methods. """
    ctx.obj = CLIConfig(verbose, store)

@click.command()
def welcome():
    """ Usage instructions for first time users. """
    try:
        f = Figlet(font='slant')
        click.secho(f.renderText("Strom-C.L.I.\n"), fg='cyan')
    except:
        click.secho("USAGE INSTRUCTIONS:\n", fg='cyan', underline=True)
        click.secho("1. dstream define -template [template filepath]\n", fg='green')
        click.secho("2. dstream load -filepath [data filepath] -token [template token file]\n", fg='green')
        click.secho("3. dstream events --all -token [template token file]\n", fg='green')
        click.pause()
    else:
        click.secho("USAGE INSTRUCTIONS:\n", fg='cyan', underline=True)
        click.secho("1. dstream define -template [template filepath]\n", fg='green')
        click.secho("2. dstream load -filepath [data filepath] -token [template token file]\n", fg='green')
        click.secho("3. dstream events --all -token [template token file]\n", fg='green')
        click.pause()

@click.command()
@click.option('-template', '-t', 'template', prompt=True, type=click.File('r'), help="Template file with required and custom fields")
@click.option('--y', is_flag=True, callback=_abort_if_false, expose_value=False, prompt="\nInitialize new DStream with this template?", help="Bypass confirmation prompt")
@click.pass_obj
def define(config, template):
    """ Upload template file for DStream. """
    template_data = template.read()
    #Try send template to server, if success...collect stream_token
    result = _api_POST(config, "define", {'template':template_data})
    if result:
        if result[0] == 200:
            token = result[1]
        else:
            click.secho("\nServer Error!...\n", fg='red', reverse=True)
            if config.verbose:
                click.secho("Server connection was denied. Check your internet connections and try again. Otherwise contact support.", fg='cyan')
    #Try load template as json and set stream_token field, if success...store tokenized template in new file
    try:
        json_template = json.loads(template_data)
        json_template['stream_token'] = token
        template_filename = os.path.basename(template.name) # NOTE: TEMP, REFACTOR OUT OF TRY
        path_list = template_filename.split('.')
        template_name = path_list[0]
        template_ext = path_list[1]
        if config.verbose:
            click.secho("File Extension found: {}".format(template_ext), fg='white')
    except:
        click.secho("\nProblem parsing template file!...\n", fg='red', reverse=True)
        if config.verbose:
            click.secho("The template file provided was not formatted correctly, please follow JSON templating guidelines.", fg='cyan')
    else:
        if config.verbose:
            click.secho("\nTemplate has been tokenized with...{}".format(json_template['stream_token']), fg='white')
        template_file = open("{}_token.txt".format(template_name), "w")
        template_file.write(json.dumps(json_template))
        template_file.close()
        if config.store:
            cli_templ = open(".cli_token", "w")
            cli_templ.write(token)
            cli_templ.close()
        click.secho("New template stored locally as '{}_token.txt'.\n".format(template_name), fg='yellow')

@click.command()
@click.option('-source', '-s', 'source', prompt=True, type=click.Choice(['kafka', 'file']), help="Specify source of data")
@click.option('--kafka-topic', default=None, help="If source is kafka, specify topic")
@click.option('-token', '-tk', 'token', default=None, type=click.File('r'), help="Tokenized template file for verification")
@click.pass_obj
def add_source(config, source, kafka_topic, token):
    """ Declare source of data: file upload or kafka stream. """
    #Check if topic was supplied when source is kafka
    if source == 'kafka' and kafka_topic == None:
        click.secho("No topic specified, please re-run command.", fg='red', reverse=True)
    else:
        if config.store:
            tk = config._set_token()
        else:
            try:
                cert = token.read()
            except:
                click.secho("Please provide a tokenized template.", fg='red', reverse=True)
                tk = None
            else:
                #Try loading template as json and retrieving token, if success...pass
                tk = _collect_token(config, cert)
        if tk:
            if config.verbose:
                click.secho("\nSending source for this DStream...\n", fg='white')
            #Try posting data to server, if success...return status_code
            result = _api_POST(config, "add-source", {'source':source, 'topic':kafka_topic, 'token':tk})

@click.command()
@click.option('-filepath', '-f', 'filepath', prompt=True, type=click.Path(exists=True), help="File-path of data file to upload")
@click.option('-token', '-tk', 'token', default=None, type=click.File('r'), help="Tokenized template file for verification")
@click.pass_obj
def load(config, filepath, token):
    """ Provide file-path of data to upload, along with tokenized_template for this DStream. """
    if config.verbose:
        click.secho("\nTokenizing data fields of {}".format(click.format_filename(filepath)), fg='white')
    if not config.store:
        try:
            cert = token.read()
        except:
            click.secho("Please provide a tokenized template.", fg='red', reverse=True)
            cert = None
    #Try load client files as json, if success...pass
    try:
        json_data = json.load(open(filepath))
    except:
        click.secho("There was an error accessing/parsing those files!...\n", fg='red', reverse=True)
        if config.verbose:
            click.secho("Data files are required to be JSON-formatted. Please supply the correct format.", fg='cyan')
    else:
        if config.store:
            tk = config._set_token()
        else:
            if cert:
                #Try collect stream_token, if success...pass
                tk = _collect_token(config, cert)
    #Try set stream_token fields to collected token, if success...pass
    try:
        with click.progressbar(json_data) as bar:
            for obj in bar:
                obj['stream_token'] = tk
    except:
        click.secho("Data file not correctly formatted!...\n", fg='red', reverse=True)
        if config.verbose:
            click.secho("Not able to set stream token fields in your data. Please supply JSON-formatted data.", fg='cyan')
    else:
        if config.verbose:
            click.secho("\nSending data...", fg='white')
        #Try send data with token to server, if success...return status_code
        result = _api_POST(config, "kafka/load", {'stream_data':json.dumps(json_data)})

#~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', default=None, type=click.File('r'), help="Tokenized template file for verification")
@click.pass_obj
def raw(config, time, utc, a, tk):
    """
    \b
     Collect all raw data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    if not config.store:
        try:
            cert = tk.read()
            token = _collect_token(config, cert)
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)
    else:
        try:
            token = config._set_token()
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)

@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', default=None, type=click.File('r'), help="Tokenized template file for verification")
@click.pass_obj
def filtered(config, time, utc, a, tk):
    """
    \b
     Collect all filtered data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    if not config.store:
        try:
            cert = tk.read()
            token = _collect_token(config, cert)
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)
    else:
        try:
            token = config._set_token()
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)

@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', default=None, type=click.File('r'), help="Tokenized template file for verification")
@click.pass_obj
def derived_params(config, time, utc, a, tk):
    """
    \b
     Collect all derived parameters for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    if not config.store:
        try:
            cert = tk.read()
            token = _collect_token(config, cert)
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)
    else:
        try:
            token = config._set_token()
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)

@click.command()
@click.option('-datetime', '-d', 'time', type=str, multiple=True, help="Datetime to collect from (YYYY-MM-DD-HH:MM:SS)")
@click.option('-utc', type=str, multiple=True, help="UTC-formatted time to collect from")
@click.option('--all', '--a', 'a', is_flag=True, is_eager=True, help="Collect all data")
@click.option('-token', '-tk', 'tk', default=None, type=click.File('r'), help="Tokenized template file for verification")
@click.pass_obj
def events(config, time, utc, a, tk):
    """
    \b
     Collect all event data for specified datetime or time-range*.
     *Options can be supplied twice to indicate a range.
    """
    if not config.store:
        try:
            cert = tk.read()
            token = _collect_token(config, cert)
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)
    else:
        try:
            token = config._set_token()
        except:
            click.secho("No token", fg='red')
        else:
            _check_options(config, "events", time, utc, a, token)


# d-stream group
dstream.add_command(welcome)
dstream.add_command(define)
dstream.add_command(add_source)
dstream.add_command(load)
#
# dstream.add_command(raw) #NOTE: TEMP
# dstream.add_command(filtered)
# dstream.add_command(derived_params)
dstream.add_command(events)
