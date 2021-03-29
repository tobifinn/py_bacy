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
from typing import Union, Dict, Any, Iterable, Tuple, List
import os.path
import glob
import time

# External modules
import prefect
from prefect import task, Task, Flow
from prefect.engine.state import State

import yaml

import pandas as pd

# Internal modules
from .system import create_folders


__all__ = [
    'PyBacyFlowTask',
    'config_reader',
    'get_parent_output',
    'construct_rundir',
    'construct_ensemble',
    'check_output_files',
    'check_number_files',
    'create_analysis_dir'
]


class PyBacyFlowTask(Task):
    def __init__(
            self,
            flow: Flow,
            name: str,
            config_path: Union[str, None] = None,
            parent_model_name: Union[str, None] = None,
            flow_kwargs: Union[Dict[str, Any], None] = None,
            **task_kwargs
    ):
        super().__init__(**task_kwargs)
        self._flow_kwargs = {}
        self.flow = flow
        self.name = name
        self.config_path = config_path
        self.parent_model_name = parent_model_name
        self.flow_kwargs = flow_kwargs

    @property
    def flow_kwargs(self) -> Dict[str, Any]:
        return self._flow_kwargs

    @flow_kwargs.setter
    def flow_kwargs(self, new_kwargs: Union[Dict[str, Any], None]):
        if new_kwargs is None:
            self._flow_kwargs = {}
        elif isinstance(new_kwargs, dict):
            self._flow_kwargs = new_kwargs
        else:
            raise TypeError('Flow kwargs have to be None or a dict!')

    def run(
            self,
            start_time: pd.Timestamp,
            analysis_time: pd.Timestamp,
            end_time: pd.Timestamp,
            cycle_config: Dict[str, Any],
            new_name: Union[None, str] = None,
            new_parent_model_name: Union[None, str] = None,
            new_config_path: Union[None, str] = None
    ) -> Union[State, None]:
        name = new_name or self.name
        parent_model_name = new_parent_model_name or self.parent_model_name
        config_path = new_config_path or self.config_path
        run_dir = construct_rundir.run(
            name=name,
            time=start_time,
            cycle_config=cycle_config
        )
        if os.path.isdir(run_dir):
            self.logger.warning(
                'Flow {0:s} already run for start_time: {1}, I\'ll skip the '
                'run!'.format(
                    name,
                    start_time
                )
            )
            flow_state = None
        else:
            self.logger.warning(
                'Starting {0:s} with start_time: {1}, analysis_time: {2}, '
                'and end_time: {3}'.format(
                    name, start_time, analysis_time, end_time
                )
            )
            run_start_time = time.time()
            flow_state = self.flow.run(
                start_time=start_time,
                analysis_time=analysis_time,
                end_time=end_time,
                cycle_config=cycle_config,
                name=name,
                config_path=config_path,
                parent_model_name=parent_model_name,
                **self.flow_kwargs
            )
            self.logger.warning(
                'Finished {0:s} with start_time: {1}, analysis_time: {2}, '
                'and end_time: {3} within {4:.1f} s'.format(
                    name, start_time, analysis_time, end_time,
                    time.time()-run_start_time
                )
            )
        return flow_state


@task
def config_reader(config_path: Union[None, str]) -> Dict[str, Any]:
    logger = prefect.context.get('logger')
    if config_path is None:
        logger.info(
            'No config path given, I will return an empty config dictionary'
        )
        return dict()
    try:
        yaml_file = open(config_path, 'r')
        config = yaml.load(yaml_file, Loader=yaml.FullLoader)
        yaml_file.close()
    except FileNotFoundError as e:
        logger.error(
            'The config file {0:s} couldn\'t be found'.format(
                config_path
            )
        )
        raise FileNotFoundError(e)
    return config


@task
def get_parent_output(
        cycle_config: Dict[str, Any],
        run_dir: str,
        ens_suffix: str,
        parent_model_name: Union[str, None] = None
) -> str:
    logger = prefect.context.get('logger')
    outer_dir = os.path.dirname(run_dir)
    analysis_dir = os.path.join(outer_dir, 'analysis')
    if parent_model_name is not None:
        parent_path = os.path.join(outer_dir, parent_model_name, 'output')
        logger.debug('Parent model output given')
    elif os.path.isdir(analysis_dir):
        parent_path = analysis_dir
        logger.debug('Analysis directory given')
    else:
        parent_path = cycle_config['EXPERIMENT']['path_init']
        logger.debug('Initial data directory given')
    parent_path = os.path.join(parent_path, ens_suffix)
    return parent_path


@task
def construct_rundir(
        name: str,
        time: pd.Timestamp,
        cycle_config: Dict[str, Any]
) -> str:
    """
    Construct a run directory structure where the run creates its input and
    output files.

    Parameters
    ----------
    name : str
        The name of the run.
    time : pd.Timestamp
        This is the starting time of the run.
    cycle_config : Dict[str, Any]
        The path of the experiment is extracted from this cycle configuration
        dictionary.

    Returns
    -------
    run_dir : str
        The constructed run dir based on the experiment path, given time,
        and given run name.
    """
    run_dir = os.path.join(
        cycle_config['EXPERIMENT']['path'],
        time.strftime('%Y%m%d_%H%M'),
        name
    )
    return run_dir


@task
def construct_ensemble(cycle_config: [str, Any]) -> Tuple[List[str], List[int]]:
    """
    Construct a list of ensemble suffixes and ensemble members.

    Parameters
    ----------
    cycle_config : Dict[str, Any]
        This cycle config is used to determine the number of ensemble members
        and if there should be a deterministic run.

    Returns
    -------
    suffixes : List[str]
        This is the list of ensemble suffixes. Each ensemble member gets
        `ens{0:03d}` as suffix, whereas a possible determinstic run is
        prepended with `det` as suffix.
    ens_range : List[int]
        This is the list of ensemble member numbers. The counting of the
        ensemble starts with 1. If a deterministic run is specified, a 0 is
        prepended to this list.
    """
    logger = prefect.context.get('logger')
    try:
        det_run = cycle_config['ENSEMBLE']['det']
        logger.debug('Determinstic run is set to {0}'.format(det_run))
    except (TypeError, KeyError) as e:
        det_run = False
        logger.debug('Couldn`t find det run keyword within cycle config')

    if det_run:
        ens_range = [0]
        suffixes = ['det']
    else:
        ens_range = []
        suffixes = []
    logger.debug(
        'Got {0} as ensemble size'.format(cycle_config['ENSEMBLE']['size'])
    )
    ens_range += list(range(1, cycle_config['ENSEMBLE']['size'] + 1))
    suffixes += [
        'ens{0:03d}'.format(mem)
        for mem in range(1, cycle_config['ENSEMBLE']['size'] + 1)
    ]
    logger.debug(
        'Construct {0} as ens range'.format(ens_range)
    )
    logger.debug(
        'Construct {0} as ens suffixes'.format(suffixes)
    )
    return suffixes, ens_range


@task
def check_output_files(
        output_folder: str,
        file_regex: Iterable[str]
) -> List[int]:
    """
    Check if given files can be found within given output folder, based on
    given file regexes.
    If no corresponding file is found, an OSError is raised.

    Parameters
    ----------
    output_folder : str
        Files within this given folder are checked.
    file_regex : Iterable[str]
        The checker iterates over these file regexes. The file regexes have
        to be evaluatable by glob.

    Returns
    -------
    n_files : list[int]
        The number of files within a directory.
    """
    n_files = []
    for regex in file_regex:
        curr_path = os.path.join(output_folder, regex)
        avail_files = list(glob.glob(curr_path))
        if not avail_files:
            raise OSError('No available files under regex {0:s} '
                          'found!'.format(curr_path))
        n_files.append(len(avail_files))
    return n_files


@task
def check_number_files(n_files: List[List[int]], output_folders: List[str]):
    """
    Check if the number of files for every found regex in a output folder
    matches with the number of files from another output folder.

    Parameters
    ----------
    n_files : List[List[int]]
        The list of the number of files for every output folder.
    output_folders : List[str]
        The output folders that were previously checked.
    """
    for k, n_files_list in enumerate(n_files):
        if not n_files_list == n_files[0]:
            raise ValueError(
                'The {0:d}-th folder has another number of files {1:s}: {2} '
                'than the first folder {3:s}: {4}'.format(
                    k, output_folders[k], n_files_list,
                    output_folders[0], n_files[0]
                )
            )


@task
def create_analysis_dir(
        cycle_config: Dict[str, Any],
        analysis_time: pd.Timestamp,
        ens_suffix: str
) -> str:
    analysis_dir = construct_rundir.run(
        name='analysis',
        time=analysis_time,
        cycle_config=cycle_config,
    )
    analysis_dir = os.path.join(analysis_dir, ens_suffix)
    analysis_dir = create_folders.run(dir_path=analysis_dir)
    return analysis_dir
