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
from prefect import Task

import yaml

# Internal modules


class ReadInConfig(Task):
    def __init__(self, config_path: Union[None, str], **kwargs):
        super().__init__(**kwargs)
        self.config_path = config_path

    def run(self) -> Dict[str, Any]:
        if self.config_path is None:
            self.logger.info('No config path given, I will return an empty '
                             'config dictionary')
            return dict()
        try:
            yaml_file = open(self.config_path, 'r')
            config = yaml.load(yaml_file)
            yaml_file.close()
        except FileNotFoundError as e:
            self.logger.error(
                'The config file {0:s} couldn\'t be found'.format(
                    self.config_path
                )
            )
            raise FileNotFoundError(e)
        return config
