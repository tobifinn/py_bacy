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
import numpy as np
import xarray as xr
import pandas as pd

from pytassim.model.terrsysmp.cosmo import preprocess_cosmo, postprocess_cosmo

# Internal modules
from .io import load_ens_data, write_ens_data
from .utils import constrain_var


logger = logging.getLogger(__name__)

EARTH_RADIUS = 6371000
DEG_TO_M = 2 * np.pi / 360 * EARTH_RADIUS

ANA_FNAME = 'laf%Y%m%d%H%M%S.nc'


def distance_func(x, y):
    try:
        diff_obs_cos_deg = y[:, :-1] - x[:-1]
    except IndexError:
        logger.error('Grid point type: {0}'.format(type(x)))
        logger.error('Grid point: {0}'.format(x))
        logger.error('Grid point shape: {0}'.format(x.shape))
        logger.error('Obs grid type: {0}'.format(type(y)))
        logger.error('Obs grid: {0}'.format(y))
        logger.error('Obs grid shape: {0}'.format(y.shape))
        raise IndexError
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


def load_constant_data(util_dir):
    file_path_const = os.path.join(util_dir, 'cosmo_const.nc')
    ds_cos_const = xr.open_dataset(file_path_const).load()
    logger.info(
        'Loaded constant COSMO data from {0:s}'.format(file_path_const)
    )
    return ds_cos_const


def load_first_guess(run_dir, fg_files, ensemble_members, start_time,
                     client=None):
    in_dir = os.path.join(
        run_dir,
        'input'
    )
    file_path_first_guess = os.path.join(
        in_dir, 'ens{0:03d}', fg_files
    )
    ds_first_guess = load_ens_data(
        file_path=file_path_first_guess,
        ensemble_members=ensemble_members,
        client=client
    )
    after_start = ds_first_guess.indexes['time'] > start_time.isoformat()
    ds_first_guess = ds_first_guess.sel(time=after_start)
    logger.info(
        'Loaded COSMO first guess data from {0:s} for {1:d} ensemble '
        'members'.format(file_path_first_guess, ensemble_members)
    )
    return ds_first_guess


def get_bg_filename(bg_files, end_time):
    return end_time.strftime(bg_files)


def get_bg_path(run_dir, bg_files, end_time):
    in_dir = os.path.join(
        run_dir,
        'input'
    )
    file_path_bg = os.path.join(
        in_dir, 'ens{0:03d}', bg_files
    )
    return file_path_bg


def load_background(run_dir, bg_files, end_time, ensemble_members,
                    client=None):
    file_path_bg = get_bg_path(run_dir, bg_files, end_time)
    ds_cos_bg = load_ens_data(
        file_path=file_path_bg, ensemble_members=ensemble_members,
        client=client
    ).load()
    logger.info(
        'Loaded COSMO background from {0:s} for {1:d} ensemble members'.format(
            file_path_bg, ensemble_members
        )
    )
    return ds_cos_bg


def write_analysis(ds_analysis, run_dir, bg_files, end_time, ensemble_size,
                   assim_vars, client=None):
    in_dir = os.path.join(run_dir, 'input')
    out_dir = os.path.join(run_dir, 'output')
    file_path_bg = os.path.join(
        in_dir, 'ens{0:03d}', bg_files
    )
    file_path_ana = os.path.join(out_dir, 'ens{0:03d}')
    write_ens_data(
        ds_analysis, file_path_bg, file_path_ana, end_time, ensemble_size,
        assim_vars,
        file_name_str=ANA_FNAME,
        client=client
    )
    logger.info(
        'Wrote analysis fo COSMO to {0:s} at {1:s} for {2:d} '
        'ensemble members'.format(
            file_path_ana, end_time.strftime('%Y%m%d-%H%M%S'), ensemble_size
        )
    )


def preprocess_array(ds_bg, ds_const, end_time, assim_vars):
    state_bg = preprocess_cosmo(ds_bg, assim_vars).load()
    return state_bg


def postprocess_array(state_analysis, ds_bg):
    return postprocess_cosmo(state_analysis, ds_bg)


def correct_vars(ds_analysis, ds_background):
    ds_analysis = constrain_var(ds_analysis, 'QV', lower_bound=0)
    ds_analysis = constrain_var(ds_analysis, 'QC', lower_bound=0)
    ds_analysis = constrain_var(ds_analysis, 'QI', lower_bound=0)
    ds_analysis = constrain_var(ds_analysis, 'QR', lower_bound=0)
    ds_analysis = constrain_var(ds_analysis, 'QS', lower_bound=0)
    ds_analysis = constrain_var(ds_analysis, 'QV_S', lower_bound=0)
    ds_analysis = constrain_var(
        ds_analysis, 'T', lower_bound=173.15, upper_bound=350
    )
    ds_analysis = constrain_var(
        ds_analysis, 'T_S', lower_bound=240, upper_bound=350
    )
    logger.info('Corrected COSMO variables')
    return ds_analysis
