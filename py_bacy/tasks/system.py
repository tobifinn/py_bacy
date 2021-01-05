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
import os
from typing import Any, Dict
import datetime

# External modules
from prefect import Task, task

# Internal modules


logger = logging.getLogger(__name__)


class CreateFolders(Task):
    def __init__(self, template: str, **kwargs):
        super().__init__(**kwargs)
        self.template = template

    def run(self, **kwargs) -> str:
        initialized_path = self.template.format(**kwargs)
        if not os.path.isdir(initialized_path):
            os.makedirs(initialized_path)
        if not os.path.isdir(initialized_path):
            raise OSError(
                'Couldn\'t initialize the directory path {0:s}'.format(
                    initialized_path
                )
            )
        return initialized_path


class SymbolicLinking(Task):
    def __init__(self, source_template: str, target_template: str, **kwargs):
        super().__init__(**kwargs)
        self.source_template = source_template
        self.target_template = target_template

    def run(self, **kwargs) -> str:
        initialized_source = self.source_template.format(**kwargs)
        initialized_target = self.target_template.format(**kwargs)
        if not os.path.exists(initialized_source):
            raise ValueError(
                'Give source path {0:s} doesn\'t exists!'.format(
                    initialized_source
                )
            )
        if os.path.isfile(initialized_target):
            self.logger.debug(
                'The target path {0:s} already exists, '
                'I\'ll remove the file'.format(
                    initialized_target
                )

            )
            os.remove(initialized_target)
        os.symlink(initialized_source, initialized_target)
        return initialized_target


@task(name='get_run_dir')
def get_run_dir(
        model_name: str,
        time: datetime.datetime,
        cycle_config: Dict[str, Any]
) -> str:
    run_dir = os.path.join(
        cycle_config['EXPERIMENT']['path'],
        time.strftime('%Y%m%d_%H%M'),
        model_name
    )
    return run_dir
