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
from prefect import task

import yaml

import pandas as pd

# Internal modules


__all__ = [
    'config_reader',
    'get_parent_output',
    'construct_rundir',
    'construct_ensemble'
]


@task
def config_reader(config_path: Union[None, str]) -> Dict[str, Any]:
    if config_path is None:
        prefect.context.logger.info(
            'No config path given, I will return an empty config dictionary'
        )
        return dict()
    try:
        yaml_file = open(config_path, 'r')
        config = yaml.load(yaml_file, Loader=yaml.FullLoader)
        yaml_file.close()
    except FileNotFoundError as e:
        prefect.context.logger.error(
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
        parent_output: Union[str, None] = None
) -> str:
    outer_dir = os.path.dirname(run_dir)
    analysis_dir = os.path.join(outer_dir, 'analysis')
    if parent_output:
        parent_path = parent_output
        prefect.context.logger.debug('Parent model output given')
    elif os.path.isdir(analysis_dir):
        parent_path = analysis_dir
        prefect.context.logger.debug('Analysis directory given')
    else:
        parent_path = cycle_config['EXPERIMENT']['path_init']
        prefect.context.logger.debug('Initial data directory given')
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
    try:
        det_run = cycle_config['ENSEMBLE']['det']
    except (TypeError, KeyError) as e:
        det_run = False

    if det_run:
        ens_range = [0]
        suffixes = ['det']
    else:
        ens_range = []
        suffixes = []
    ens_range += list(range(1, cycle_config['ENSEMBLE']['size'] + 1))
    suffixes += [
        'ens{0:03d}'.format(mem)
        for mem in range(1, cycle_config['ENSEMBLE']['size'] + 1)
    ]
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
