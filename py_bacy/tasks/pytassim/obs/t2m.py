#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 15.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
from typing import Dict, Any, List, Tuple, Union
import datetime
import os.path
import glob

# External modules
import prefect
from prefect import task

import xarray as xr
from distributed import Client
import pandas as pd
import numpy as np

from pytassim.model.terrsysmp.cosmo import preprocess_cosmo, postprocess_cosmo
from pytassim.obs_ops.terrsysmp import CosmoT2mOperator

# Internal modules
from py_bacy.tasks.cosmo import get_cos_bg_fname
from py_bacy.tasks.general import check_output_files
from py_bacy.tasks.io import load_ens_data
from py_bacy.tasks.system import symlink


__all__ = [
    'link_first_guess',
    'load_obs',
    'load_first_guess'
]


@task
def link_first_guess(
        parent_model_output: str,
        input_folder: str,
        config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        analysis_time: datetime.datetime
) -> List[str]:
    fg_file_path = os.path.join(parent_model_output, config['obs']['fg_files'])
    fg_files = glob.glob(fg_file_path)
    linked_files = []
    for fpath in fg_files:
        fg_fname = os.path.basename(fpath)
        tmp_fg_path = os.path.join(input_folder, fg_fname)
        target_file = symlink.run(fpath, tmp_fg_path)
        linked_files.append(target_file)
    return linked_files


@task
def load_first_guess(
        fg_files: List[str],
        obs_window: Tuple[datetime.datetime, datetime.datetime],
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        ens_members: List[int],
        client: Client,
) -> xr.DataArray:
    ds_first_guess = load_ens_data(
        file_path=fg_files,
        client=client
    )
    ds_first_guess['ensemble'] = ens_members
    ds_first_guess = ds_first_guess.sel(
        time=slice(obs_window[0], obs_window[1])
    )
    ds_first_guess = preprocess_cosmo(ds_first_guess, ['T', 'T_2M']).load()
    return ds_first_guess


@task
def load_obs_file(
        file_path_obs: str
) -> xr.Dataset:
    logger = prefect.context.get('logger')
    ds_obs_raw = xr.open_dataset(file_path_obs).load()
    ds_obs = ds_obs_raw.drop_vars(ds_obs_raw.attrs['multiindex'])
    ds_obs['obs_grid_1'] = pd.MultiIndex.from_frame(
        ds_obs_raw[ds_obs_raw.attrs['multiindex']].to_dataframe()[
            ds_obs_raw.attrs['multiindex']])
    ds_obs.attrs = []
    logger.info('Loaded observations from {0:s}'.format(file_path_obs))
    return ds_obs


@task
def load_stations(
        utils_dir: str
) -> pd.DataFrame:
    file_path_station = os.path.join(utils_dir, 'stations.hd5')
    df_stations = pd.read_hdf(file_path_station, 'stations')
    return df_stations


@task
def load_coords(
    ds_with_coords: Union[xr.Dataset, xr.DataArray]
) -> np.ndarray:
        coords_latlon = np.stack(
            (ds_with_coords['lat'].values, ds_with_coords['lon'].values),
            axis=-1
        )
        return coords_latlon


@task
def load_constant_data(
        utils_dir: str
) -> xr.Dataset:
    file_path_const = os.path.join(utils_dir, 'cosmo_const.nc')
    ds_cos_const = xr.open_dataset(file_path_const).load()
    return ds_cos_const


@task
def load_obs(
        obs_window: Tuple[datetime.datetime, datetime.datetime],
        assim_config: Dict[str, Any],
        cycle_config: Dict[str, Any],
        client: Client,
) -> List[xr.Dataset]:
    ds_obs = load_obs_file.run(
        file_path_obs=assim_config['obs']['path']
    )
    ds_obs = ds_obs.sel(time=slice(obs_window[0], obs_window[1]))
    df_stations = load_stations.run(
        utils_dir=assim_config['obs']['utils_path']
    )
    ds_const_data = load_constant_data.run(
        utils_dir=assim_config['obs']['utils_path']
    )
    coords_array = load_coords.run(
        ds_with_coords=ds_const_data
    )
    obs_operator = CosmoT2mOperator(
        df_stations, coords_array, ds_const_data
    )
    ds_obs.obs.operator = obs_operator
    return ds_obs
