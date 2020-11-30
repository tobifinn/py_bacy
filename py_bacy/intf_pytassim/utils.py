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
import datetime
import argparse
from collections import OrderedDict

# External modules
import numpy as np
import pandas as pd
import xarray as xr


# Internal modules
from .io import write_df


logger = logging.getLogger(__name__)


_height_vars = ['level', 'level1', 'levlak', 'levsno', 'levtot', 'numrad',
                'height_2m', 'height_10m', 'height_toa', 'soil1']


def describe_arr(array, var_name):
    logger.info(array.dims)
    try:
        dim_height = [d for d in array.dims if d in _height_vars][0]
        vertical_vals = array[dim_height].values
        logger.info('Found {0:s} as vertical axis'.format(dim_height))
        logger.info('Vertical values: {0}'.format(vertical_vals))
    except IndexError:
        array = array.expand_dims('level')
        vertical_vals = [0, ]
    dims_data = [d for d in array.dims if d not in _height_vars]
    logger.info('Dims data: {0}'.format(dims_data))
    var_min = array.min(dim=dims_data).to_pandas()
    var_max = array.max(dim=dims_data).to_pandas()
    var_mean = array.mean(dim=dims_data).to_pandas()
    var_std = array.std(dim=dims_data).to_pandas()
    var_per = array.quantile([0.1, 0.5, 0.9], dim=dims_data).T.to_pandas()
    var_per.columns = ['10 %', 'median', '90 %']
    var_per.index = np.arange(len(vertical_vals))
    var_mai = np.abs(array).mean(dim=dims_data).to_pandas()
    var_df = pd.DataFrame(
        data={'min': var_min, 'max': var_max, 'mean': var_mean,
              'std': var_std, 'MAI': var_mai},
        index=np.arange(len(vertical_vals)))
    var_df = pd.concat([var_df, var_per], axis=1)
    logger.info(var_name)
    logger.info(vertical_vals)
    logger.info(var_df.index)
    var_df.index = pd.MultiIndex.from_product(
        [[var_name, ], vertical_vals],
        names=['var_name', 'height']
    )
    return var_df


def write_info_df(array, filename, assim_vars, run_dir):
    info_df = pd.DataFrame(
        columns=['min', 'mean', 'max', 'std', '10 %', 'median', '90 %',
                 'MAI']
    )
    for var in assim_vars:
        var_df = describe_arr(array[var], var)
        var_df = var_df.reindex(info_df.columns, axis=1)
        info_df = pd.concat([info_df, var_df], axis=0)
    write_df(info_df, run_dir, filename)


def info_assimilation(ds_analysis, ds_background, assim_vars, run_dir,
                      suffix='cosmo'):
    analysis_mean = ds_analysis.mean('ensemble')
    background_mean = ds_background.mean('ensemble')
    impact = analysis_mean - background_mean
    write_info_df(impact, 'info_impact_{0:s}.txt'.format(suffix), assim_vars,
                  run_dir)
    write_info_df(background_mean, 'info_background_{0:s}.txt'.format(suffix),
                  assim_vars, run_dir)
    write_info_df(analysis_mean, 'info_analysis_{0:s}.txt'.format(suffix),
                  assim_vars, run_dir)


def describe_diff_mean(arr):
    avail_dims = arr.dims[1:]
    abs_diff = np.abs(arr)
    diff_mean = arr.mean(dim=avail_dims).to_pandas()
    diff_min = arr.min(dim=avail_dims).to_pandas()
    diff_max = arr.max(dim=avail_dims).to_pandas()
    diff_std = arr.std(dim=avail_dims).to_pandas()
    diff_rmse = np.sqrt((arr**2).mean(dim=avail_dims)).to_pandas()
    diff_mae = abs_diff.mean(dim=avail_dims).to_pandas()
    diff_per = arr.quantile(
        [0.05, 0.1, 0.5, 0.9, 0.95], dim=avail_dims
    ).T.to_pandas()
    diff_per.columns = ['5%', '10 %', 'median', '90 %', '95%']
    diff_df = pd.DataFrame(
        data={
            'min': diff_min, 'mean': diff_mean, 'max': diff_max,
            'std': diff_std, 'mae': diff_mae, 'rmse': diff_rmse,
        },
    )
    diff_df = pd.concat([diff_df, diff_per], axis=1)
    return diff_df


def write_mean_statistics(fg_grouped, obs_grouped, run_dir, suffix):
    statistics = OrderedDict()
    fg_mean = fg_grouped.mean('ensemble')
    diff_mean = obs_grouped['observations'] - fg_mean
    statistics['mean'] = describe_diff_mean(diff_mean)
    diff_mean_time = diff_mean.stack(
        group_time=['obs_group', 'time']
    ).transpose('group_time', 'obs_grid_1')
    statistics['mean time'] = describe_diff_mean(diff_mean_time)
    obs_time = obs_grouped['observations'].stack(
        group_time=['obs_group', 'time']
    ).transpose('group_time', 'obs_grid_1')
    statistics['obs time'] = describe_diff_mean(obs_time)
    fg_time = fg_mean.stack(
        group_time=['obs_group', 'time']
    ).transpose('group_time', 'obs_grid_1')
    statistics['fg time'] = describe_diff_mean(fg_time)
    write_df(
        statistics['mean'], run_dir=run_dir,
        filename='innov_mean_{0:s}.txt'.format(suffix)
    )
    write_df(
        statistics['mean time'], run_dir=run_dir,
        filename='innov_mean_{0:s}.txt'.format(suffix)
    )
    write_df(
        statistics['obs time'], run_dir=run_dir,
        filename='obs_mean_{0:s}.txt'.format(suffix)
    )
    write_df(
        statistics['fg time'], run_dir=run_dir,
        filename='obs_mean_{0:s}.txt'.format(suffix)
    )


def info_obs_diagonstics(ds_first_guess, observations, run_dir, suffix='cosmo'):
    from .plot import write_obs_plots

    obs_equivalent = []
    filtered_observations = []
    for obs in observations:
        try:
            obs_equivalent.append(obs.obs.operator(obs, ds_first_guess))
            filtered_observations.append(obs)
        except NotImplementedError:
            pass
    print('ds fg:', ds_first_guess[1, 0])
    print('pseudo obs:', obs_equivalent[0])
    fg_grouped = xr.concat(obs_equivalent, dim='obs_group')
    obs_grouped = xr.concat(filtered_observations, dim='obs_group')
    obs_grouped['observations'] = obs_grouped['observations'].drop('ensemble')
    print('Grouped observations:', obs_grouped)
    write_mean_statistics(fg_grouped, obs_grouped, run_dir, suffix)
    write_obs_plots(fg_grouped, obs_grouped, run_dir, suffix)
    logger.info('Written observation diagnostics')


def constrain_var(array, var_name, lower_bound=None, upper_bound=None):
    array = array.copy()
    var_array = array[var_name]
    if lower_bound is not None:
        var_array = var_array.where(var_array > lower_bound, lower_bound)
    if upper_bound is not None:
        var_array = var_array.where(var_array < upper_bound, upper_bound)
    array[var_name] = var_array
    return array


def parse_datetime(dt_str):
    try:
        return datetime.datetime.strptime(dt_str, '%Y%m%d_%H%M')
    except ValueError:
        msg = 'Given date string `{0:s}` is not a valid date'.format(
            dt_str
        )
        raise argparse.ArgumentTypeError(msg)
