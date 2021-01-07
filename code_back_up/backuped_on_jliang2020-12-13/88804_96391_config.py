import os
import json

this_dir = os.path.dirname(__file__)
config_filename = os.path.join(this_dir, '../config/config.json')
config_file = open(config_filename)
config = json.load(config_file)
config_file.close()
api_version = 'v2.11'
