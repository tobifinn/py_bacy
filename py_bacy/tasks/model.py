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
from typing import Dict, Any, Tuple, Union, List
import subprocess

# External modules
from prefect import Task

# Internal modules
from .system import CreateFolders


__all__ = [
    'ModifyNamelist',
    'InitializeNamelist',
    'CreateDirectoryStructure',
    'ConstructEnsemble'
]


class ModifyNamelist(Task):
    @staticmethod
    def get_template(template_path) -> str:
        with open(template_path, mode='r') as template_file:
            template = template_file.read()
        return template

    def run(
            self,
            template_path: str,
            placeholder_dict: Dict[str, Any]
    ) -> str:
        template = self.get_template(template_path)
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

    def run(self, namelist: str, ens_mem: int = 0, **path_kwargs) -> str:
        target_path = self.path_template.format(**path_kwargs)
        self.write_template(namelist=namelist, target_path=target_path)
        _ = subprocess.Popen([target_path, '{0:d}'.format(ens_mem)])
        target_folder = os.path.basename(target_path)
        return target_folder


class CreateDirectoryStructure(CreateFolders):
    def run(
            self,
            run_dir: str,
            ens_mem: Union[str, int] = 0
    ) -> Tuple[str, str]:
        pass


class ConstructEnsemble(Task):
    @staticmethod
    def _deterministic_run(cycle_config: Dict[str, Any]) -> bool:
        try:
            return cycle_config['ENSEMBLE']['det']
        except (TypeError, KeyError) as e:
            return False

    def run(self, cycle_config: Dict[str, Any]) -> Tuple[List[str], List[int]]:
        if self._deterministic_run(cycle_config):
            ens_range = [0]
            suffixes = ['det']
        else:
            ens_range = []
            suffixes = []
        ens_range += list(range(1, cycle_config['ENSEMBLE']['size']+1))
        suffixes += [
            'ens{0:03d}'.format(mem)
            for mem in range(1, cycle_config['ENSEMBLE']['size']+1)
        ]
        return suffixes, ens_range
