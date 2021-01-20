#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 14.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
from typing import Dict, Any, List, Tuple
import os.path
import glob

# External modules
import prefect
from prefect import task

import xarray as xr
import numpy as np
from distributed import Client
import pandas as pd

from pytassim.model.terrsysmp.cosmo import preprocess_cosmo, postprocess_cosmo

# Internal modules
from py_bacy.tasks.cosmo import get_cos_bg_fname
from py_bacy.tasks.io import load_ens_data, write_ens_data
from py_bacy.tasks.system import symlink
from py_bacy.tasks.xarray import constrain_var


EARTH_RADIUS = 6371000
DEG_TO_M = 2 * np.pi / 360 * EARTH_RADIUS

ANA_FNAME = 'laf%Y%m%d%H%M%S.nc'


__all__ = [
    'link_background',
    'load_background',
    'post_process_analysis',
    'write_analysis',
    'link_output'
]


def distance_func(x, y):
    diff_obs_cos_deg = y[:, :-1] - x[:-1]
    diff_obs_cos_m = diff_obs_cos_deg * DEG_TO_M
    dist_obs_cos_2d = np.sqrt(np.sum(diff_obs_cos_m**2, axis=-1))

    cos_press = press_int(x[-1])
    obs_press = press_int(y[:, -1])
    obs_lnp = np.log(obs_press)
    cos_lnp = np.log(cos_press)
    dist_obs_cos_vert = np.abs(cos_lnp - obs_lnp)
    return dist_obs_cos_2d, dist_obs_cos_vert


def press_int(level_height):
    return 1013.25 * np.power(1 - (0.0065 * level_height / 288.15), 5.255)


@task
def link_background(
    parent_model_output: str,
    input_folder: str,
    config: Dict[str, Any],
    cycle_config: Dict[str, Any],
    analysis_time: pd.Timestamp
) -> str:
    logger = prefect.context.get('logger')
    bg_fname = get_cos_bg_fname.run(
        fname_template=config['bg_files'], curr_time=analysis_time
    )
    source_path = os.path.join(parent_model_output, bg_fname)
    source_files_found = list(sorted(glob.glob(source_path)))
    logger.debug('Found {0} as background files'.format(source_files_found))
    source_file = source_files_found[0]
    target_file = os.path.join(input_folder, bg_fname)
    target_file = symlink.run(source=source_file, target=target_file)
    return target_file


@task
def load_background(
        bg_files: List[str],
        analysis_time: pd.Timestamp,
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        ens_members: List[int],
        client: Client,
) -> Tuple[xr.Dataset, xr.DataArray]:
    ds_cosmo = load_ens_data.run(
        file_paths=bg_files, client=client
    )
    ds_cosmo['ensemble'] = ens_members
    background = preprocess_cosmo(ds_cosmo, assim_config['assim_vars'])
    return ds_cosmo, background


@task
def post_process_analysis(
        analysis: xr.DataArray,
        model_dataset: xr.Dataset
) -> xr.Dataset:
    analysis = postprocess_cosmo(analysis, model_dataset)
    analysis = constrain_var.run(analysis, 'QV', lower_bound=0)
    analysis = constrain_var.run(analysis, 'QC', lower_bound=0)
    analysis = constrain_var.run(analysis, 'QI', lower_bound=0)
    analysis = constrain_var.run(analysis, 'QR', lower_bound=0)
    analysis = constrain_var.run(analysis, 'QS', lower_bound=0)
    analysis = constrain_var.run(analysis, 'QV_S', lower_bound=0)
    analysis = constrain_var.run(
        analysis, 'T', lower_bound=173.15, upper_bound=350
    )
    analysis = constrain_var.run(
        analysis, 'T_S', lower_bound=240, upper_bound=350
    )
    return analysis


@task
def write_analysis(
        analysis: xr.Dataset,
        background_files: List[str],
        output_dirs: List[str],
        analysis_time: pd.Timestamp,
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        client: Client,
) -> Tuple[List[str], xr.Dataset]:
    loaded_analysis = analysis.compute(client=client)

    analysis_fname = analysis_time.strftime(ANA_FNAME)
    analysis_files = [
        os.path.join(output_dir, analysis_fname) for output_dir in output_dirs
    ]
    _ = write_ens_data.run(
        dataset_to_write=loaded_analysis,
        source_paths=background_files,
        target_paths=analysis_files,
        assim_vars=assim_config['assim_vars'],
        client=client
    )
    return analysis_files, loaded_analysis


@task
def link_output(
        background_file: str,
        output_dir: str,
        analysis_time: pd.Timestamp
) -> str:
    if not os.path.isfile(background_file):
        raise OSError('{0:s} does not exist!'.format(background_file))

    output_fname = analysis_time.strftime(ANA_FNAME)
    output_file = os.path.join(output_dir, output_fname)
    output_file = symlink.run(
        source=background_file, target=output_file
    )
    return output_file
