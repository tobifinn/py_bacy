#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 13.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import logging
from typing import Dict, Any, Tuple, List, Iterable, Union
import os.path
import glob

# External modules
import prefect
from prefect import task

import xarray as xr
import pandas as pd
from distributed import Client

from pytassim.interface.base import BaseAssimilation

# Internal modules
from py_bacy.tasks.system import symlink


logger = logging.getLogger(__name__)


__all__ = [
    'get_observation_window',
    'link_files',
    'assimilate',
    'align_obs_first_guess',
    'link_analysis',
    'default_post_process_obs'
]


@task(name='get_observation_window')
def get_observation_window(
        start_time: pd.Timestamp,
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any]
) -> Tuple[pd.Timestamp, pd.Timestamp]:
    """
    Construct an observation window from given timestamp and dictionaries.

    Parameters
    ----------
    start_time : pd.Timestamp
        This is the basis time from where the time delta, specifing
        the relative assimilation window, is defined.
    assim_config : Dict[str, Any]
        This dicitionary defined the assimilation configuration.
        The relative time deltas for the start and end of the window are
        specified Within [obs][td_start] and [obs][td_end], respecetively.
    cycle_config : Dict[str, Any]
        The lead time of the assimilation cycling [TIME][cycle_lead_time]
        will be used if no relative time window is specified within the
        assimilation configuration.

    Returns
    -------
    obs_times : Tuple[pd.Timestamp, pd.Timestamp]
    """
    logger = prefect.context.get('logger')
    lead_timedelta = pd.to_timedelta(
        cycle_config['TIME']['cycle_lead_time'], unit='S'
    )
    try:
        obs_timedelta = [
            pd.to_timedelta(assim_config['obs']['td_start']),
            pd.to_timedelta(assim_config['obs']['td_end']),
        ]
    except KeyError:
        obs_timedelta = [
            pd.Timedelta('0h'),
            lead_timedelta
        ]
    obs_times = (start_time + obs_timedelta[0], start_time + obs_timedelta[1])
    return obs_times


@task(name='link_first_guess')
def link_first_guess(
        input_dir: str,
        parent_analysis_dir: str,
        assim_config: Dict[str, Any],
        ens_suffix: str
) -> List[str]:
    fg_file_path = os.path.join(
        parent_analysis_dir, ens_suffix, assim_config['obs']['fg_files']
    )
    fg_files = list(glob.glob(fg_file_path))
    linked_fg_files = []
    for fpath in fg_files:
        fg_fname = os.path.basename(fpath)
        tmp_trg_path = os.path.join(input_dir, fg_fname)
        symlink(fpath, tmp_trg_path)
        linked_fg_files.append(tmp_trg_path)
    return linked_fg_files


@task(name='link_files')
def link_files(
        fnames: Iterable[str],
        src_dir: str,
        target_dir: str,
) -> List[str]:
    src_files_path = [
        os.path.join(src_dir, fname) for fname in fnames
    ]
    src_files = [
        list(sorted(glob.glob(path))) for path in src_files_path
    ]
    src_files_flattened = [
        file_path for found_files in src_files for file_path in found_files
    ]
    linked_files = []
    for src_path in src_files_flattened:
        fname = os.path.basename(src_path)
        trg_path = os.path.join(target_dir, fname)
        symlink(src_path, trg_path)
        linked_files.append(trg_path)
    return linked_files


@task
def assimilate(
        assimilation: BaseAssimilation,
        background: xr.DataArray,
        observations: Union[xr.Dataset, Iterable[xr.Dataset]],
        first_guess: xr.DataArray,
        analysis_time: Any,
) -> xr.DataArray:
    analysis = assimilation.assimilate(
        state=background,
        observations=observations,
        pseudo_state=first_guess,
        analysis_time=analysis_time
    )
    return analysis


@task
def align_obs_first_guess(
        observations: xr.Dataset,
        first_guess: xr.DataArray
) -> Tuple[xr.Dataset, xr.DataArray]:
    time_intersection = observations.indexes['time'].intersection(
        first_guess.indexes['time']
    )
    sliced_obs = observations.sel(time=time_intersection)
    sliced_first_guess = first_guess.sel(time=time_intersection)
    sliced_obs.obs.operator = observations.obs.operator
    return sliced_obs, sliced_first_guess


@task
def link_analysis(
        output_file: str,
        analysis_folder: str
):
    fname_analysis = os.path.basename(output_file)
    if not os.path.isfile(output_file):
        raise OSError('{0:s} does not exist!'.format(output_file))
    analysis_file = os.path.join(analysis_folder, fname_analysis)
    analysis_file = symlink.run(source=output_file, target=analysis_file)
    return analysis_file


@task
def default_post_process_obs(
        observations: List[xr.Dataset],
        first_guess: Union[None, xr.DataArray] = None
) -> Tuple[List[xr.Dataset], Union[None, xr.DataArray]]:
    return observations, first_guess
