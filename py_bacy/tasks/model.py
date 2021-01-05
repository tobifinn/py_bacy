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
import os.path
from typing import Dict, Any
import subprocess

# External modules
from prefect import Task

# Internal modules


class ModifyNamelist(Task):
    def __init__(self, namelist_template: str, **kwargs):
        super().__init__(**kwargs)
        self.namelist_template = namelist_template

    def get_template(self) -> str:
        with open(self.namelist_template, mode='r') as template_file:
            template = template_file.read()
        return template

    def run(self, placeholder_dict: Dict[str, Any]) -> str:
        template = self.get_template()
        for placeholder, value in placeholder_dict.items():
            template = template.replace(placeholder, value)
        return template


class InitializeNamelist(Task):
    def __init__(self, path_template: str, **kwargs):
        super().__init__(**kwargs)
        self.path_template = path_template

    def write_template(self, namelist: str, target_path: str) -> None:
        with open(target_path, mode='w') as target_file:
            target_file.write(namelist)
        subprocess.call(['chmod', '755', target_path])

    def run(self, namelist: str, ens_member: int = 0, **path_kwargs) -> str:
        target_path = self.path_template.format(**path_kwargs)
        self.write_template(namelist=namelist, target_path=target_path)
        _ = subprocess.Popen([target_path, str(ens_member)])
        target_folder = os.path.basename(target_path)
        return target_folder
