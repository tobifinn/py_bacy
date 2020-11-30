#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 5/21/19
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2019}  {Tobias Sebastian Finn}
#

# System modules
import logging
import os

# External modules
import xarray as xr
import cartopy.crs as ccrs
import numpy as np
import pandas as pd

from pytassim.model.terrsysmp.clm import preprocess_clm, postprocess_clm

# Internal modules
from .cosmo import DEG_TO_M
from .io import load_ens_data, write_ens_data
from .utils import constrain_var


logger = logging.getLogger(__name__)

rotated_pole = ccrs.RotatedPole(pole_longitude=-171.0, pole_latitude=41.5)
plate_carree = ccrs.PlateCarree()

ANA_FNAME = 'clm_ana%Y%m%d%H%M%S.nc'
DENSITY = 1000


def distance_func(x, y):
    grid_hori = rotated_pole.transform_point(x[1], x[0], plate_carree)[::-1]
    diff_obs_clm_deg = y[:, :-1] - grid_hori
    diff_obs_clm_m = diff_obs_clm_deg * DEG_TO_M
    dist_obs_clm_2d = np.sqrt(np.sum(diff_obs_clm_m**2, axis=-1))

    dist_obs_clm_vert = np.abs(y[:, -1]-x[-1])
    return dist_obs_clm_2d, dist_obs_clm_vert


def get_bg_filename(bg_files, end_time):
    timediff_clm = end_time - end_time.replace(hour=0, minute=0, second=0)
    timediff_clm_secs = timediff_clm.total_seconds()
    clm_file_name = 'clmoas.clm2.r.{0:s}-{1:05d}.nc'.format(
        end_time.strftime('%Y-%m-%d'), int(timediff_clm_secs)
    )
    return clm_file_name


def get_bg_path(run_dir, bg_files, end_time):
    in_dir = os.path.join(
        run_dir,
        'input'
    )
    clm_filename = get_bg_filename(bg_files, end_time)
    file_path_bg = os.path.join(in_dir, 'ens{0:03d}', clm_filename)
    return file_path_bg


def load_background(run_dir, bg_files, end_time, ensemble_members, client=None):
    file_path_bg = get_bg_path(run_dir, bg_files, end_time)
    ds_clm_bg = load_ens_data(
        file_path=file_path_bg, ensemble_members=ensemble_members,
        client=client
    ).load()
    logger.info(
        'Loaded CLM background from {0:s} for {1:d} ensemble members'.format(
            file_path_bg, ensemble_members
        )
    )
    return ds_clm_bg


def load_constant_data(util_dir):
    file_path_const = os.path.join(util_dir, 'clm_const.nc')
    ds_clm_const = xr.open_dataset(file_path_const).load()
    logger.info(
        'Loaded constant CLM data from {0:s}'.format(file_path_const)
    )
    return ds_clm_const


def load_auxiliary_data(util_dir):
    file_path_aux = os.path.join(util_dir, 'clm_aux.nc')
    ds_clm_aux = xr.open_dataset(file_path_aux)
    ds_clm_aux = ds_clm_aux.stack(column=['lat', 'lon'])
    logger.info(
        'Loaded auxiliary CLM data from {0:s}'.format(file_path_aux)
    )
    return ds_clm_aux


def write_analysis(ds_analysis, run_dir, bg_files, end_time, ensemble_size,
                   assim_vars, client=None):
    in_dir = os.path.join(run_dir, 'input')
    out_dir = os.path.join(run_dir, 'output')
    file_path_bg = os.path.join(
        in_dir, 'ens{0:03d}', get_bg_filename(bg_files, end_time)
    )
    file_path_ana = os.path.join(out_dir, 'ens{0:03d}')
    write_ens_data(
        ds_analysis, file_path_bg, file_path_ana, end_time,
        ensemble_size, assim_vars,
        file_name_str=ANA_FNAME,
        client=None
    )
    logger.info(
        'Wrote analysis fo CLM to {0:s} at {1:s} for {2:d} '
        'ensemble members'.format(
            file_path_ana, end_time.strftime('%Y%m%d-%H%M%S'), ensemble_size
        )
    )


def preprocess_array(ds_bg, ds_const, end_time, assim_vars):
    state_bg = preprocess_clm(ds_bg, assim_vars)
    state_bg = state_bg.expand_dims('time', axis=1)
    state_bg['time'] = [end_time, ]
    grid_index = pd.MultiIndex.from_product(
        [ds_const['lat'].values, ds_const['lon'].values,
         ds_const['levels'].values], names=['lat', 'lon', 'vgrid']
    )
    state_bg['grid'] = grid_index
    return state_bg


def postprocess_array(state_analysis, ds_bg):
    return postprocess_clm(state_analysis, ds_bg)


def correct_vars(ds_analysis, ds_background):
    tot_water_bg = ds_background['WA'] + ds_background['H2OSOI_LIQ'].sum(
        'levtot') + ds_background['H2OCAN'].values
    tot_water_ana = ds_analysis['WA'] + ds_analysis['H2OSOI_LIQ'].sum(
        'levtot') + ds_analysis['H2OCAN'].values
    delta_water = tot_water_ana - tot_water_bg
    ds_analysis['WA'] = ds_analysis['WA']-delta_water
    ds_analysis = constrain_var(ds_analysis, 'WA', lower_bound=0)
    ds_analysis = constrain_var(
        ds_analysis, 'H2OSOI_LIQ', lower_bound=0
    )
    ds_analysis = constrain_var(ds_analysis, 'H2OCAN', lower_bound=0)
    #ds_analysis = constrain_var(
    #    ds_analysis, 'T_SOISNO', lower_bound=250, upper_bound=350
    #)
    logger.info('Corrected CLM variables')
    return ds_analysis
