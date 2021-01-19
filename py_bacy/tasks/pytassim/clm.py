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

from distributed import Client
import cartopy.crs as ccrs
import xarray as xr
import pandas as pd
import numpy as np

from pytassim.model.terrsysmp.clm import preprocess_clm, postprocess_clm

# Internal modules
from .cosmo import DEG_TO_M
from ..clm import get_clm_bg_fname
from ..io import load_ens_data, write_ens_data
from ..system import symlink
from ..xarray import constrain_var


rotated_pole = ccrs.RotatedPole(pole_longitude=-171.0, pole_latitude=41.5)
plate_carree = ccrs.PlateCarree()

ANA_FNAME = 'clm_ana%Y%m%d%H%M%S.nc'
DENSITY = 1000


__all__ = [
    'link_background',
    'link_analysis',
    'load_background',
    'post_process_analysis',
    'write_analysis'
]


def distance_func(x, y):
    grid_hori = rotated_pole.transform_point(x[1], x[0], plate_carree)[::-1]
    diff_obs_clm_deg = y[:, :-1] - grid_hori
    diff_obs_clm_m = diff_obs_clm_deg * DEG_TO_M
    dist_obs_clm_2d = np.sqrt(np.sum(diff_obs_clm_m**2, axis=-1))

    dist_obs_clm_vert = np.abs(y[:, -1]-x[-1])
    return dist_obs_clm_2d, dist_obs_clm_vert


@task
def link_background(
    parent_model_output: str,
    input_folder: str,
    config: Dict[str, Any],
    cycle_config: Dict[str, Any],
    analysis_time: pd.Timestamp
) -> str:
    logger = prefect.context.get('logger')
    bg_fname = get_clm_bg_fname.run(
        curr_time=analysis_time
    )
    source_path = os.path.join(parent_model_output, bg_fname)
    source_files_found = list(sorted(glob.glob(source_path)))
    logger.debug('Found {0} as background files'.format(source_files_found))
    source_file = source_files_found[0]
    target_file = os.path.join(input_folder, bg_fname)
    target_file = symlink.run(source=source_file, target=target_file)
    return target_file


@task
def load_clm_restart_files(
        bg_files: List[str],
        ens_members: List[int],
) -> xr.Dataset:
    ds_clm = load_ens_data.run(
        file_paths=bg_files,
    )
    ds_clm['ensemble'] = ens_members
    return ds_clm


@task
def load_clm_grid(
    utils_path: str
) -> xr.Dataset:
    file_path_const = os.path.join(utils_path, 'clm_const.nc')
    ds_clm_const = xr.open_dataset(file_path_const).load()
    grid_index = pd.MultiIndex.from_product(
        [ds_clm_const['lat'].values, ds_clm_const['lon'].values,
         ds_clm_const['levels'].values], names=['lat', 'lon', 'vgrid']
    )
    return grid_index


@task
def restart_files_to_background(
        ds_clm: xr.Dataset,
        assim_vars: List[str],
        analysis_time: pd.Timestamp,
        grid_index: pd.MultiIndex
) -> xr.DataArray:
    background = preprocess_clm(ds_clm, assim_vars)
    background = background.expand_dims('time', axis=1)
    background['time'] = [analysis_time, ]
    background['grid'] = grid_index
    return background


@task
def load_background(
        bg_files: List[str],
        analysis_time: pd.Timestamp,
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        ens_members: List[int],
        client: Client,
) -> Tuple[xr.Dataset, xr.DataArray]:
    ds_clm = load_clm_restart_files.run(
        bg_files=bg_files,
        ens_members=ens_members
    )
    grid_index = load_clm_grid.run(
        utils_path=assim_config['obs']['utils_path']
    )
    background = restart_files_to_background.run(
        ds_clm=ds_clm,
        assim_vars=assim_config['assim_vars'],
        analysis_time=analysis_time,
        grid_index=grid_index
    )
    return ds_clm, background


@task
def post_process_analysis(
        analysis: xr.DataArray,
        model_dataset: xr.Dataset
) -> xr.Dataset:
    analysis = postprocess_clm(analysis, model_dataset)
    tot_water_bg = model_dataset['WA'] + model_dataset['H2OSOI_LIQ'].sum(
        'levtot') + model_dataset['H2OCAN'].values
    tot_water_ana = analysis['WA'] + analysis['H2OSOI_LIQ'].sum(
        'levtot') + analysis['H2OCAN'].values
    delta_water = tot_water_ana - tot_water_bg
    analysis['WA'] = analysis['WA']-delta_water
    analysis = constrain_var.run(analysis, 'WA', lower_bound=0)
    analysis = constrain_var.run(analysis, 'H2OSOI_LIQ', lower_bound=0)
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
        loaded_analysis,
        background_files,
        analysis_files,
        assim_config['assim_vars'],
        client=client
    )
    return analysis_files, loaded_analysis
