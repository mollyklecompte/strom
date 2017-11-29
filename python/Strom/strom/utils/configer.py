"""
setup system wide strom configuration class
"""

from configparser import (ConfigParser,
                          DEFAULTSECT,
                          ParsingError,
                          NoSectionError,
                          MissingSectionHeaderError,
                          DuplicateSectionError,
                          NoOptionError,
                          InterpolationError,
                          ExtendedInterpolation,
                          Error as ConfigParserGeneralError)
from argparse import (ArgumentParser,
                      ArgumentError,
                      ArgumentTypeError)
from sys import argv as sysargs
from os.path import expanduser
from logging import (DEBUG, INFO)

from strom.utils.logger.logger import logger


__author__ = "Parham <parham@tura.io>"


# public members
SECTION_SEPARATOR = '/'

# default config files to load
DEFAULT_CONFIG_FILES = [expanduser('~/.strom/strom.ini'),
                        './strom.ini']


class Configer(dict):
    """
    Base class to load configuration parameters from files or command line arguments

    You can use this class as a dict to set and read config params:
    Configer["option"] = "value"

    You can load configuration options from a file:
    Configer.load(file_path)

    Configuration file support variable interpolation. Example:
    option1=value1
    option2=other ${option1}

    In this example option2 would have the value "other value1"

    *Note:* Configuration file is divided into sections and must have a DEFAULT section. Example:
    [DEFAULT]
    option1=value1

    [OTHER_SECTION]
    option2=value2

    To read options including section as a dict you can use the following syntax:
    Configer["OTHER_SECTION/option2"]

    Section names are separated by '/' from the option name. This works for both setting and reading options.
    """

    def __init__(self, config_files=None, parse_sysargs=False, **kwargs):
        super().__init__()
        self._cfg = ConfigParser(interpolation=ExtendedInterpolation())
        self._files = []
        self._loaded_files = []

        # if file name(s) are provided then load them
        if config_files is not None:
            # check if we got a list of files
            if isinstance(config_files, list):
                for file in config_files:
                    if isinstance(file, str):
                        self.add_file_path(file)
            # check if we got a single file path
            elif isinstance(config_files, str):
                self.add_file_path(config_files)
            # load everything
            self.load()

        # parsing sys args if needed
        if parse_sysargs:
            self.parse_args()

        # set all options in kwargs
        for key in kwargs:
            self.set(key, kwargs[key])

    def load(self, file_name=None):
        if file_name is None:
            for f in self._files:
                self.__load(f)
        else:
            self.__load(file_name)

    def __load(self, file_name):
        logger.info("Loading configuration file: %s" % file_name)
        try:
            with open(file_name, 'r') as fp:
                try:
                    self._cfg.read_file(fp)
                    self._loaded_files.append(file_name)
                    logger.info("Configuration loaded successfully.")
                except (ParsingError, NoSectionError, MissingSectionHeaderError, ConfigParserGeneralError) as err:
                    logger.warn("Bad configuration file. File: %s. Error: %s" % (file_name, err.message))
        except (PermissionError, FileNotFoundError) as err:
            logger.info("Could not read configuration file: %s. %s" % (file_name, str(err)))

    def parse_args(self, args=None):
        logger.info("Parsing args for configuration params. args = %s" % (args if args is not None else sysargs[1:]))
        try:
            parser = ArgumentParser()
            parser.add_argument("-f", "--config_file", action="append", dest="files")
            parser.add_argument("-o", "--option", action="append", dest="options")
            parser.add_argument("-D", "--param", action="append", dest="options")
            if args is None:
                args, unknown = parser.parse_known_args()
            else:
                args, unknown = parser.parse_known_args(args)
            files = args.files
            options = args.options
            if files is not None and isinstance(files, list):
                for file in files:
                    try:
                        self.load(file)
                    except:
                        logger.warn("Error in loading configuration file: %s" % file)
            if options is not None and isinstance(options, list):
                for option in options:
                    try:
                        key, value = self.__read_option_keyvalue(option)
                        self.set(key, value)
                    except:
                        logger.warn("Could not load configuration argument: %s" % option)
        except (ArgumentError, ArgumentTypeError) as err:
            logger.warn("Bad configuration arguments. %s" % err.message)
            raise
        except:
            logger.warn("Unknown configuration argument error.")
            raise

    def loaded_files(self):
        return self._loaded_files

    def add_file_path(self, file_name):
        self._files.append(file_name)

    def get_file_paths(self):
        return self._files

    def remove_file_path(self, file_name):
        self._files.remove(file_name)

    def add_section(self, section):
        try:
            logger.debug("Adding configuration section: %s" % section)
            self._cfg.add_section(section)
        except (DuplicateSectionError, ConfigParserGeneralError):
            pass

    def remove_section(self, section):
        try:
            logger.debug("Removing configuration section: %s" % section)
            self._cfg.remove_section(section)
        except ConfigParserGeneralError:
            pass

    def get(self, option, section=None, default=None):
        try:
            if section is None:
                option, section = self.__get_option_name(option)
            value = self._cfg.get(section, option)
        except (NoSectionError, NoOptionError, ConfigParserGeneralError) as err:
            logger.info("Configuration parameter didn't exist, returning the default value." % err.message)
            return default
        logger.debug("Read configuration parameter: (section=%s) %s=%s" % (section, option, value))
        return value

    def set(self, option, value, section=None):
        logger.debug("setting config param: (section=%s) %s=%s" % (section, option, value))
        try:
            if section is None:
                option, section = self.__get_option_name(option)
            self._cfg.set(section, option, value)
        except NoSectionError:
            logger.debug("config section '%s' didn't exist. Section was created." % section)
            self.add_section(section)
            try:
                self._cfg.set(section, option, value)
            except ConfigParserGeneralError as err:
                logger.warn("Unexpected error while setting the configuration parameter. %s" % err.message)
        except (InterpolationError, ConfigParserGeneralError, ValueError) as err:
            logger.warn("Unexpected error while setting the configuration parameter. %s" % err.message)

    def store(self, file_name):
        logger.info("Saving configuration to file: %s" % file_name)
        with open(file_name, 'w') as f:
            self._cfg.write(f)

    def print_options(self, level=DEBUG):
        """
        Print all set configuration parameters to the logger. Default log level is DEBUG (10)

        :param level: logger level to print to. Default is DEBUG (10)
        :type level: log level (DEBUG=10, INFO=20, WARN=30)
        :return:
        """
        cfg = self._cfg
        logger.log(level, "Listing configuration options...")
        logger.log(level, "  section=" + DEFAULTSECT)
        for (key, value) in cfg.items(section=DEFAULTSECT):
            logger.log(level, "   %s = %s" % (str(key), str(value)))
        for section in cfg.sections():
            logger.log(level, "  section=%s" % section)
            for option in list(cfg._sections[str(section)].keys()):
                logger.log(level, "   %s = %s" % (option, cfg.get(section, option)))

    def __setitem__(self, key, value):
        option, section = self.__get_option_name(key)
        self.set(option, str(value), section)

    def __getitem__(self, item):
        option, section = self.__get_option_name(item)
        return self.get(option, section)

    @staticmethod
    def __get_option_name(option_str):
        section = DEFAULTSECT
        option = str(option_str).lstrip().rstrip()
        if option.find(SECTION_SEPARATOR) > 0:
            if option.count(SECTION_SEPARATOR) == 1:
                section, t, option = option.partition(SECTION_SEPARATOR)
            else:
                msg = "Bad configuration option name: '%s'. Contains more than one section separator '%s'" % (str(option_str), SECTION_SEPARATOR)
                logger.warn(msg)
                raise AttributeError(msg)
        return option, section

    @staticmethod
    def __read_option_keyvalue(option_str):
        option = str(option_str).lstrip().rstrip()
        if option.count('=') >= 1:
            k, t, v = option.partition('=')
            return k, v
        else:
            return option, None


# define global configer class
configer = Configer(config_files=DEFAULT_CONFIG_FILES, parse_sysargs=True)

# print configuration parameters if log level is INFO or above
if logger.level <= INFO:
    configer.print_options(INFO)

# set default import items
__all__ = ["configer", "Configer", "SECTION_SEPARATOR"]
