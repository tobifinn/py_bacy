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
        self.flow = flow
        self.name = name
        self.config_path = config_path
        self.parent_model_name = parent_model_name
        self.flow_kwargs = flow_kwargs

    def run(
            self,
            start_time: pd.Timestamp,
            analysis_time: pd.Timestamp,
            end_time: pd.Timestamp,
            cycle_config: Dict[str, Any]
    ) -> State:
        self.logger.info(
            'Starting {0:s} with start_time: {1}, analysis_time: {2}, '
            'and end_time: {3}'.format(
                self.name, start_time, analysis_time, end_time
            )
        )
        flow_state = self.flow.run(
            start_time=start_time,
            analysis_time=analysis_time,
            end_time=end_time,
            cycle_config=cycle_config,
            name=self.name,
            config_path=self.config_path,
            parent_model_name=self.parent_model_name,
            **self.flow_kwargs
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
) -> str:
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
    output_folder : str
        The given output folder which was checked.
    """
    for regex in file_regex:
        curr_path = os.path.join(output_folder, regex)
        avail_files = list(glob.glob(curr_path))
        if not avail_files:
            raise OSError('No available files under regex {0:s} '
                          'found!'.format(curr_path))
    return output_folder


@task
def create_analysis_dir(
        cycle_config: Dict[str, Any],
        analysis_time: pd.Timestamp,
        ens_suffix: str
) -> str:
    analysis_dir = construct_rundir(
        name='analysis',
        time=analysis_time,
        cycle_config=cycle_config,
    )
    analysis_dir = os.path.join(analysis_dir, ens_suffix)
    analysis_dir = create_folders.run(dir_path=analysis_dir)
    return analysis_dir
