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
import os.path

# External modules
import prefect
from prefect import Task, Flow

import yaml

# Internal modules


__all__ = [
    'ReadInConfig',
    'ParentGetter'
]


class ReadInConfig(Task):
    def run(self, config_path: Union[None, str]) -> Dict[str, Any]:
        if config_path is None:
            self.logger.info(
                'No config path given, I will return an empty config dictionary'
            )
            return dict()
        try:
            yaml_file = open(config_path, 'r')
            config = yaml.load(yaml_file, Loader=yaml.FullLoader)
            yaml_file.close()
        except FileNotFoundError as e:
            self.logger.error(
                'The config file {0:s} couldn\'t be found'.format(
                    config_path
                )
            )
            raise FileNotFoundError(e)
        return config


class ParentGetter(Task):
    def run(
            self,
            cycle_config: Dict[str, Any],
            run_dir: str,
            ens_suffix: str,
            parent_output: Union[str, None] = None
    ) -> str:
        outer_dir = os.path.dirname(run_dir)
        analysis_dir = os.path.join(outer_dir, 'analysis')
        if parent_output:
            parent_path = parent_output
        elif os.path.isdir(analysis_dir):
            parent_path = analysis_dir
        else:
            parent_path = cycle_config['EXPERIMENT']['path_init']
        parent_path = os.path.join(parent_path, ens_suffix)
        return parent_path


class FlowRunner(Task):
    def __init__(self, flow: Flow, **kwargs):
        super().__init__(**kwargs)
        self.flow = flow

    def run(self, *args, **kwargs) -> str:
        return self.flow.run(*args, **kwargs)
