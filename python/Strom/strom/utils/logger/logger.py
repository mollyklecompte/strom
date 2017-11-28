import logging
import logging.config
import json

config_dict = json.load(open("logger_config.json"))
logging.config.dictConfig(config_dict)
logger = logging.getLogger("stromLogger")
