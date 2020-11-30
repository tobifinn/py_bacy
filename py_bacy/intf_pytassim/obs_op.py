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

# External modules
from pytassim.obs_ops.terrsysmp.cos_t2m import CosmoT2mOperator
from pytassim.model.terrsysmp.cosmo import preprocess_cosmo

# Internal modules
from . import cosmo
from .io import load_coords, load_observations, load_stations, load_ens_data


logger = logging.getLogger(__name__)


def prepare_obs_op_t2m(ds_first_guess, ds_obs, df_stations, ds_const_data,
                       coords_fg):
    obs_operator = CosmoT2mOperator(
        df_stations, coords_fg, ds_const_data
    )
    time_intersection = ds_obs.indexes['time'].intersection(
        ds_first_guess.indexes['time']
    )
    print('Intersected time: ', time_intersection)
    prepared_obs = ds_obs.sel(time=time_intersection)
    prepared_obs.obs.operator = obs_operator

    prepared_state = ds_first_guess.sel(time=time_intersection)
    prepared_state = preprocess_cosmo(prepared_state, ['T', 'T_2M']).load()
    return prepared_state, prepared_obs, obs_operator


def load_obs_fg_t2m(run_dir, fg_files, ensemble_members,
                    start_time, file_path_obs, util_dir, client=None):
    ds_fg = cosmo.load_first_guess(run_dir, fg_files, ensemble_members,
                                   start_time, client=client)
    ds_obs = load_observations(file_path_obs)
    df_stations = load_stations(util_dir)
    ds_const_data = cosmo.load_constant_data(util_dir)
    coords_fg = load_coords(ds_fg)
    prepared_fg, prepared_obs, obs_op = prepare_obs_op_t2m(
        ds_fg, ds_obs, df_stations, ds_const_data, coords_fg
    )
    logger.info('Loaded observations and first guess for T2m from COSMO')
    return prepared_fg, prepared_obs, obs_op
