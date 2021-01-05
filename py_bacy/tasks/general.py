#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 05.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import logging
from typing import Union, Dict, Any

# External modules
import prefect
from prefect import task

import yaml

# Internal modules


@task('read_in_config')
def read_in_config(config_path: Union[None, str]) -> Dict[str, Any]:
    logger = prefect.context.get("logger")
    if config_path is None:
        logger.info('No config path given, I will return an empty config '
                    'dictionary')
        return dict()
    try:
        yaml_file = open(config_path, 'r')
        config = yaml.load(yaml_file)
        yaml_file.close()
    except FileNotFoundError as e:
        logger.error(
            'The config file {0:s} couldn\'t be found'.format(
                config_path
            )
        )
        raise FileNotFoundError(e)
    return config
