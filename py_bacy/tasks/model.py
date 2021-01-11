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
from typing import Dict, Any, Tuple, Union, List, Iterable
import subprocess
import glob
import datetime

# External modules
from prefect import Task
import pandas as pd

# Internal modules
from .general import FlowRunner, ReadInConfig


__all__ = [
    'ModifyNamelist',
    'InitializeNamelist',
    'CreateDirectoryStructure',
    'ConstructEnsemble',
    'CheckOutput',
    'RestartModelFlowRunner'
]


class ModifyNamelist(Task):
    @staticmethod
    def get_template(template_path) -> str:
        with open(template_path, mode='r') as template_file:
            template = template_file.read()
        return template

    def run(
            self,
            model_config: Dict[str, Any],
            placeholder_dict: Dict[str, Any]
    ) -> str:
        template = self.get_template(model_config['template'])
        for placeholder, value in placeholder_dict.items():
            template = template.replace(placeholder, str(value))
        return template


class InitializeNamelist(Task):
    def __init__(self, namelist_name: str, **kwargs):
        super().__init__(**kwargs)
        self.namelist_name = namelist_name

    def write_template(self, namelist: str, target_path: str) -> None:
        with open(target_path, mode='w') as target_file:
            target_file.write(namelist)
        subprocess.call(['chmod', '755', target_path])

    def run(
            self,
            namelist: str,
            input_folder: str,
            mem: int=0,
            **path_kwargs
    ):
        target_path = os.path.join(input_folder, self.namelist_name)
        self.write_template(namelist=namelist, target_path=target_path)
        _ = subprocess.check_call([target_path, '{0:d}'.format(mem)])
        return target_path


class CreateDirectoryStructure(Task):
    def __init__(self, directories: Iterable[str], **kwargs: Any):
        super().__init__(**kwargs)
        self.directories = directories

    @staticmethod
    def _create_folder(initialized_path):
        if not os.path.isdir(initialized_path):
            os.makedirs(initialized_path)
        if not os.path.isdir(initialized_path):
            raise OSError(
                'Couldn\'t initialize the directory path {0:s}'.format(
                    initialized_path
                )
            )
        return initialized_path

    def run(
            self,
            run_dir: str,
            ens_suffix: str,
    ) -> Dict[str, str]:
        structure = dict()
        for dir_name in self.directories:
            dir_path = os.path.join(run_dir, dir_name, ens_suffix)
            _ = self._create_folder(dir_path)
            structure[dir_name] = dir_path
        return structure


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


class CheckOutput(Task):
    def __init__(self, output_regex: Iterable[str], **kwargs):
        super().__init__(**kwargs)
        self.output_regex = output_regex

    def run(self, output_folder: str) -> str:
        for regex in self.output_regex:
            curr_path = os.path.join(output_folder, regex)
            avail_files = list(glob.glob(curr_path))
            if not avail_files:
                raise OSError('No available files under regex {0:s} '
                              'found!'.format(curr_path))
        return output_folder


class RestartModelFlowRunner(FlowRunner):
    def __init__(self, model_flow, **kwargs):
        super().__init__(flow=model_flow, **kwargs)

    def _get_timerange(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            restart_td: List[str]
    ) -> List[datetime.datetime]:
        model_steps = [pd.to_datetime(start_time), ]
        self.logger.debug('Got {0} as restart_td'.format(restart_td))
        for td in restart_td[:-1]:
            try:
                curr_td = pd.to_timedelta(td)
                new_step = model_steps[-1] + curr_td
            except ValueError:
                raise ValueError('Couldn\'t convert {0:s} into '
                                 'timedelta'.format(td))
            model_steps.append(new_step)
        last_steps = pd.date_range(model_steps[-1], end_time,
                                   freq=restart_td[-1])
        model_steps = model_steps + list(last_steps[1:])
        model_steps = [step.to_pydatetime() for step in model_steps]
        return model_steps

    def run(self,
            name: str,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            run_dir: str,
            config_path: str,
            cycle_config: Dict[str, Any],
            parent_output: Union[str, None] = None,
            **kwargs
    ) -> str:
        model_config = ReadInConfig().run(config_path)
        model_steps = self._get_timerange(
            start_time, end_time, model_config['restart_td']
        )
        curr_parent_output = parent_output
        curr_restart = False
        for i, curr_model_start_time in enumerate(model_steps[:-1]):
            try:
                curr_end_time = model_steps[i+1]
            except KeyError:
                curr_end_time = end_time
            _ = super().run(
                start_time=curr_model_start_time,
                end_time=curr_end_time,
                parent_output=curr_parent_output,
                restart=curr_restart,
                config_path=config_path,
                cycle_config=cycle_config,
                name=name,
                **kwargs
            )
            curr_restart = True
            curr_parent_output = os.path.join(run_dir, 'output')
        return os.path.join(run_dir, 'output')
