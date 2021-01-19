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
from typing import List, Union
from shutil import copyfile

# External modules
import prefect
from prefect import task

import dask
from distributed import Client, get_client
import netCDF4 as nc4
import xarray as xr
import numpy as np
from tqdm.autonotebook import tqdm

# Internal modules


@task
def load_data_from_paths(
        file_paths: List[str]
) -> xr.Dataset:
    """
    Load data from given file paths in NetCDF-4 format.

    Parameters
    ----------
    file_paths : List[str]

    Returns
    -------
    loaded_ds : xr.Dataset
    """
    loaded_ds = xr.open_mfdataset(
        file_paths, parallel=True, combine='nested',
        concat_dim='time', decode_cf=True, decode_times=True,
        data_vars='minimal', coords='minimal', compat='override'
    )
    return loaded_ds


@task
def load_ens_data(
        file_paths: Union[List[str], List[List[str]]],
) -> xr.Dataset:
    """
    Load ensemble data with xarray and dask from given file paths.
    The dataset will be concatenated along a new unnumbered ensemble dimension.

    Parameters
    ----------
    file_paths : List[str] or List[List[str]]
        The items of this list will be passed to `xarray.open_mfdataset`.
        All ensemble members have to result to the same ensemble structure.

    Returns
    -------
    ds_ens : xr.Dataset
        The loaded dataset with a concatenated ensemble dimension, which is
        unnumbered.
    """
    logger = prefect.context.get('logger')
    logger.debug('Source file paths: {0}'.format(file_paths))
    ds_ens_list = []
    pbar_paths = tqdm(file_paths)
    for mem_paths in pbar_paths:
        ds_mem = load_data_from_paths.run(file_paths=mem_paths)
        ds_ens_list.append(ds_mem)
    logger.info('Starting to concat ensemble')
    ds_ens = xr.concat(ds_ens_list, dim='ensemble')
    return ds_ens


def write_single_ens_mem(
        source_path: str,
        target_path: str,
        analysis_dataset: xr.Dataset,
        assim_vars: List[str]
) -> str:
    """
    Write a single ensemble member where the source and target path are
    specified.
    Variables that are specified within the `assim_vars` list are overwritten
    with the corresponding excerpts of the analysis dataset.

    Parameters
    ----------
    source_path : str
        This netCDF4-file will be copied to the target path.
    target_path : str
        The netCDF4-file will be created in this target path and manipulated
        with given analysis dataset.
    analysis_dataset : xr.Dataset
        This dataset specifies the analysis data that should be written to
        the target path.
    assim_vars : List[str]
        These list of assimilation variables specifies which variables were
        changed during the assimilation process.

    Returns
    -------
    target_path : str
        The target path with the written data.
    """
    copyfile(source_path, target_path)
    with nc4.Dataset(target_path, mode='r+') as loaded_ds:
        for var_name in assim_vars:
            loaded_ds[var_name][:] = analysis_dataset[var_name]
    return target_path


@task
def write_ens_data(
        dataset_to_write: xr.Dataset,
        source_paths: List[str],
        target_paths: List[str],
        assim_vars: List[str],
        client: Union[None, Client] = None
) -> str:
    """
    Write a given dataset with ensemble members to given target paths.
    The source path is used as base netCDF4-file and will be copied to the
    target paths.

    Parameters
    ----------
    dataset_to_write : xr.Dataset
        This dataset will be written to the target paths.
        The number of ensemble members have to be the same as the length of
        the source paths and target paths.
    source_paths : List[str]
        Each item of this source path list is used as base file and will be
        copied to the target paths.
        The length of this list has to be the same as the ensemble dimension
        within `dataset_to_write`.
    target_paths : List[str]
        For each ensemble member, the corresponding dataset will be written
        to these target paths.
        The base-file of these target paths are specified within the source
        paths argument.
        The length of this list has to be the same as the ensemble dimension
        within `dataset_to_write`.
    assim_vars : List[str]
        This list specifies the variable name that were changed during the
        assimilation.
    client : None or distributed.Client, optional
        This client is used to write the analysis for each ensemble member in
        paralllel.

    Returns
    -------
    target_paths : str
        The target paths with the written analyses.
    """
    logger = prefect.context.get('logger')
    if client is None:
        logger.warning('No client was given, I try to infer the client')
        client = get_client(timeout=10)
    ens_delayed_list = []
    for member_num, source_path in enumerate(source_paths):
        dataset_scattered = client.scatter(
            dataset_to_write.isel(ensemble=member_num)
        )
        tmp_delayed = dask.delayed(write_single_ens_mem)(
            source_path, target_paths[member_num], dataset_scattered, assim_vars
        )
        ens_delayed_list.append(tmp_delayed)
    ens_delayed_list = client.compute(ens_delayed_list)
    _ = client.gather(ens_delayed_list)
    logger.debug(
        'Finished writing of ensemble data to {0}'.format(target_paths)
    )
    return target_paths
