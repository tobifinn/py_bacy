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
import pickle as pk
import glob
from shutil import copyfile

# External modules
import numpy as np
import xarray as xr
import pandas as pd
import netCDF4 as nc4
from tabulate import tabulate
from tqdm import tqdm

import distributed
from distributed.diagnostics import progress
import dask

# Internal modules


logger = logging.getLogger(__name__)


def load_ens_data(file_path, ensemble_members, client=None):
    logger.debug('Source file path: {0:s}'.format(file_path.format(1)))
    ens_mems_list = np.arange(1, ensemble_members+1)
    if client is None:
        logger.warning('No client was given, I try to infer the client')
        client = distributed.get_client(timeout=10)
    ds_ens_list = []
    pbar_mem = tqdm(ens_mems_list)
    for mem in pbar_mem:
        path_mem = file_path.format(mem)
        found_paths = sorted(list(glob.glob(path_mem)))
        ds_mem = xr.open_mfdataset(
            found_paths, parallel=True, combine='nested',
            concat_dim='time', decode_cf=True, decode_times=True,
            data_vars='minimal', coords='minimal', compat='override'
        )
        ds_ens_list.append(ds_mem)
    logger.info('Starting to concat ensemble')
    ds_ens = xr.concat(ds_ens_list, dim='ensemble')
    ds_ens = ds_ens.chunk({'ensemble': 1})
    ds_ens['ensemble'] = ens_mems_list
    del ds_ens_list
    return ds_ens


def load_coords(ds):
    coords_latlon = np.stack(
        (ds['lat'].values, ds['lon'].values),
        axis=-1
    )
    return coords_latlon

#
# def load_coords(util_dir, coords_name):
#     file_path_coords = os.path.join(util_dir, coords_name)
#     coords_latlon = np.stack(
#         pk.load(open(file_path_coords, 'rb'))._calc_lat_lon(), axis=-1
#     )
#     logger.info(
#         'Loaded grid coords from {0:s}'.format(file_path_coords)
#     )
#     return coords_latlon


def load_stations(util_dir):
    file_path_station = os.path.join(util_dir, 'stations.hd5')
    df_stations = pd.read_hdf(file_path_station, 'stations')
    logger.info(
        'Loaded station information from {0:s}'.format(file_path_station)
    )
    return df_stations


def load_observations(file_path_obs):
    ds_obs_raw = xr.open_dataset(file_path_obs).load()
    ds_obs = ds_obs_raw.drop_vars(ds_obs_raw.attrs['multiindex'])
    ds_obs['obs_grid_1'] = pd.MultiIndex.from_frame(
        ds_obs_raw[ds_obs_raw.attrs['multiindex']].to_dataframe()[
            ds_obs_raw.attrs['multiindex']])
    ds_obs.attrs = []
    logger.info('Loaded observations from {0:s}'.format(file_path_obs))
    return ds_obs


def write_weights(run_dir, ds_weights, suffix=''):
    out_dir = os.path.join(run_dir, 'output')
    file_path_weights = os.path.join(
        out_dir, 'ens_weights_{0:s}.nc'.format(suffix)
    )
    ds_weights.to_netcdf(file_path_weights)
    logger.info('Written ensemble weights to {0:s}'.format(file_path_weights))


def write_single_ens_mem(path_src_mem, path_trg_mem, ana_ds, assim_vars):
    copyfile(path_src_mem, path_trg_mem)
    logger.info(
        'Copied file {0:s} to {1:s}'.format(path_src_mem, path_trg_mem)
    )
    with nc4.Dataset(path_trg_mem, mode='r+') as mem_ds:
        for var_name in assim_vars:
            mem_ds[var_name][:] = ana_ds[var_name]
            logger.info(
                'Overwritten {0:s} in {1:s} with new data'.format(
                    var_name, path_trg_mem
                )
            )


def write_ens_data(ens_ds, source_path, target_path, end_time,
                   ens_mems, assim_vars, client=None,
                   file_name_str='laf%Y%m%d%H%M%S.nc'
                   ):
    ens_mems_list = np.arange(1, ens_mems+1)
    ens_delayed_list = []
    if client is None:
        logger.warning('No client was given, I try to infer the client')
        client = distributed.get_client(timeout=10)
    for mem in ens_mems_list:
        path_src_mem = list(sorted(glob.glob(source_path.format(mem))))[0]
        trg_dir = target_path.format(mem)
        path_trg_mem = os.path.join(
            trg_dir, end_time.strftime(file_name_str)
        )
        ana_ds = client.scatter(ens_ds.sel(ensemble=mem))
        tmp_delayed = dask.delayed(write_single_ens_mem)(
            path_src_mem, path_trg_mem, ana_ds, assim_vars
        )
        ens_delayed_list.append(tmp_delayed)
    ens_delayed_list = client.compute(ens_delayed_list)
    _ = client.gather(ens_delayed_list)
    logger.info(
        'Finished writing of ensemble data to {0:s}'.format(target_path)
    )


def write_df(info_df, run_dir, filename):
    try:
        info_text = tabulate(info_df, headers='keys', tablefmt='psql') + '\n'
    except TypeError as e:
        print(info_df)
        raise e
    out_dir = os.path.join(run_dir, 'output')
    file_path_info = os.path.join(out_dir, filename)
    with open(file_path_info, 'a+') as fh_info:
        fh_info.write(info_text)
