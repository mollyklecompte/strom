import logging
import logging.config
import json
import os
config_dict = json.load(open(os.path.dirname(__file__)+"/logger_config.json"))
logger = logging.getLogger("stromLogger")
logging.config.dictConfig(config_dict)
