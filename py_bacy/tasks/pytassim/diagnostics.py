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
import logging
from typing import Iterable, Tuple, List, Dict, Any
import os.path
from collections import OrderedDict

# External modules
import prefect
from prefect import task

import numpy as np
import xarray as xr
import pandas as pd
from tabulate import tabulate

from distributed import Client


# Internal modules


__all__ = [
    'info_observations',
    'info_assimilation'
]


@task
def info_observations(
        first_guess: xr.DataArray,
        observations: Iterable[xr.Dataset],
        run_dir: str,
        client: Client
):
    from .plot import write_obs_plots

    obs_equivalent, filtered_obs = apply_obs_operator(
        first_guess=first_guess, observations=observations
    )
    statistics = get_obs_mean_statistics(
        obs_equivalent=obs_equivalent, filtered_obs=filtered_obs
    )
    for name, df in statistics.items():
        write_df(
            df, run_dir=run_dir,
            filename='innov_mean_{0:s}.txt'.format(name)
        )
    write_obs_plots(obs_equivalent, filtered_obs, run_dir)


@task
def info_assimilation(
        analysis: xr.Dataset,
        background: xr.Dataset,
        run_dir: str,
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        client: Client
):
    analysis_mean = analysis.mean('ensemble')
    background_mean = background.mean('ensemble')
    impact = analysis_mean - background_mean
    write_info_df(
        impact, 'info_impact.txt', assim_config['assim_vars'], run_dir
    )
    write_info_df(
        background_mean, 'info_background.txt', assim_config['assim_vars'],
        run_dir
    )
    write_info_df(
        analysis_mean, 'info_analysis.txt', assim_config['assim_vars'], run_dir
    )


def apply_obs_operator(
        first_guess: xr.DataArray,
        observations: Iterable[xr.Dataset]
) -> Tuple[xr.DataArray, xr.DataArray]:
    obs_equivalent = []
    filtered_observations = []
    for obs in observations:
        try:
            obs_equivalent.append(obs.obs.operator(obs, first_guess))
            filtered_observations.append(obs['observations'])
        except NotImplementedError:
            pass
    obs_equivalent = xr.concat(obs_equivalent, dim='obs_group')
    filtered_obs = xr.concat(filtered_observations, dim='obs_group')
    return obs_equivalent, filtered_obs


def write_df(
        info_df: pd.DataFrame,
        run_dir: str,
        filename: str
) -> str:
    try:
        info_text = tabulate(info_df, headers='keys', tablefmt='psql') + '\n'
    except TypeError as e:
        raise e
    out_dir = os.path.join(run_dir, 'output')
    file_path_info = os.path.join(out_dir, filename)
    with open(file_path_info, 'a+') as fh_info:
        fh_info.write(info_text)
    return file_path_info


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


def get_obs_mean_statistics(
        obs_equivalent: xr.DataArray,
        filtered_obs: xr.DataArray
):
    statistics = OrderedDict()
    fg_mean = obs_equivalent.mean('ensemble')
    diff_mean = filtered_obs - fg_mean
    statistics['mean'] = describe_diff_mean(diff_mean)
    diff_mean_time = diff_mean.stack(
        group_time=['obs_group', 'time']
    ).transpose('group_time', 'obs_grid_1')
    statistics['mean_time'] = describe_diff_mean(diff_mean_time)
    obs_time = filtered_obs.stack(
        group_time=['obs_group', 'time']
    ).transpose('group_time', 'obs_grid_1')
    statistics['obs_time'] = describe_diff_mean(obs_time)
    fg_time = fg_mean.stack(
        group_time=['obs_group', 'time']
    ).transpose('group_time', 'obs_grid_1')
    statistics['fg_time'] = describe_diff_mean(fg_time)
    return statistics


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
