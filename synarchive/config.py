#!/usr/bin/env python

####################
# Required Modules #
####################

# Generic
import json
import os
import pkg_resources
from collections import OrderedDict
from glob import glob

# Libs


# Custom


##################
# Configurations #
##################

TEMPLATE_DIR = pkg_resources.resource_filename("synarchive", "templates")

###########
# Helpers #
###########

def detect_configurations(dirname):
    """ Automates loading of configuration files in specified directory

    Args:
        dirname (str): Target directory to load configurations from
    Returns:
        Params (dict)
    """

    def parse_filename(filepath):
        """ Extracts filename from a specified filepath
            Assumptions: There are no '.' in filename
        
        Args:
            filepath (str): Path of file to parse
        Returns:
            filename (str)
        """
        return os.path.basename(filepath).split('.')[0]

    # Load in parameters for participating servers
    config_globstring = os.path.join(TEMPLATE_DIR, "**/*.json")
    config_paths = glob(config_globstring)

    return {parse_filename(c_path): c_path for c_path in config_paths}

######################
# Database Templates #
######################

template_paths = detect_configurations(TEMPLATE_DIR)

SCHEMAS = {}
for name, s_path in template_paths.items():
    with open(s_path, 'r') as schema:
        SCHEMAS[name] = json.load(schema, object_pairs_hook=OrderedDict)
