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
import datetime
import os.path
import glob

# External modules
import prefect
from prefect import task

import xarray as xr
import numpy as np
from distributed import Client

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
    'link_analysis',
    'load_background',
    'post_process_analysis',
    'write_analysis'
]


@task
def link_background(
    parent_model_output: str,
    input_folder: str,
    config: Dict[str, Any],
    cycle_config: Dict[str, Any],
    analysis_time: datetime.datetime
) -> str:
    logger = prefect.context.get('logger')
    bg_fname = get_cos_bg_fname.run(
        fname_template=config['bg_files'], analysis_time=analysis_time
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
        analysis_time: datetime.datetime,
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        ens_members: List[int],
        client: Client,
) -> Tuple[xr.Dataset, xr.DataArray]:
    ds_cosmo = load_ens_data.run(
        file_path=bg_files,
    )
    ds_cosmo['ensemble'] = ens_members
    background = preprocess_cosmo(ds_cosmo, assim_config['assim_vars'])
    return ds_cosmo, background


@task
def post_process_analysis(
        analysis: xr.DataArray,
        model_dataset: xr.DataArray
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
        analysis_time: datetime.datetime,
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        client: Client,
) -> xr.Dataset:
    analysis = analysis.sel(time=[analysis_time])
    loaded_analysis = analysis.compute(client=client)

    analysis_fname = analysis_time.strftime(ANA_FNAME)
    analysis_files = [
        os.path.join(output_dir, analysis_fname) for output_dir in output_dirs
    ]
    _ = write_ens_data.run(
        loaded_analysis,
        background_files,
        analysis_files,
        assim_config['assim_vars'],
        client=client
    )
    return loaded_analysis


@task
def link_analysis(
        output_folder: str,
        analysis_folder: str,
        analysis_time: datetime.datetime,
):
    fname_analysis = analysis_time.strftime(ANA_FNAME)
    output_file = os.path.join(output_folder, fname_analysis)
    if not os.path.isfile(output_file):
        raise OSError('{0:s} does not exist!'.format(output_file))
    analysis_file = os.path.join(analysis_folder, fname_analysis)
    analysis_file = symlink.run(source=output_file, target=analysis_file)
    return analysis_file
