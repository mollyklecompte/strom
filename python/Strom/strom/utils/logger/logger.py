import logging
import logging.config
import json
import os
config_dict = json.load(open(os.path.dirname(__file__)+"/logger_config.json"))
logging.config.dictConfig(config_dict)
logger = logging.getLogger("stromLogger")
