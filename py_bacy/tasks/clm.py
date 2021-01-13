#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 13.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import typing
import os.path

# External modules
from prefect import task

import pandas as pd

# Internal modules
from .system import symlink


__all__ = [
    'link_clm_restart',
    'link_clm_initial',
    'get_clm_bg_fname'
]


@task
def link_clm_restart(
        parent_model_output: str,
        output_fname: str,
        input_folder: str
) -> str:
    """
    Link clm_in-files to start CÖ; from a given output folder into the given
    input folder for the case of a internal model restart without any data
    assimilation.

    Parameters
    ----------
    parent_model_output : str
        This is the source folder where the clm_in-file is stored.
    output_fname : str
        Name of the laf-file within the source folder. Due to data
        assimilation, the name of the file might be different than clm_in*.
    input_folder : str
        The laf file is linked into this input folder.

    Returns
    -------
    clm_target : str
        This is the target path, where the clm_in-file was linked to.
    """
    clm_source = os.path.join(parent_model_output, output_fname)
    clm_target = os.path.join(input_folder, 'clm_in.nc')
    symlink.run(clm_source, clm_target)
    return clm_target


@task
def link_clm_initial(
        parent_model_output: str,
        model_start_time: pd.Timestamp,
        input_folder: str
) -> str:
    """
    Link clm_in files to start CLM from a given output folder into the given
    input folder for the case of a fresh initial start of CLM.

    Parameters
    ----------
    parent_model_output : str
        This is the source folder where the laf-file is stored.
    input_folder : str
        The laf file is linked into this input folder.
    model_start_time : pd.Timestamp
        This is the model start time, which is used to construct the
        clm_in-file.

    Returns
    -------
    clm_target : str
        This is the target path, where the clm_in-file was linked to.
    """
    clm_out_file = model_start_time.strftime(
        'clm_ana%Y%m%d%H%M%S.nc'
    )
    clm_source = os.path.join(parent_model_output, clm_out_file)
    clm_target = os.path.join(input_folder, 'clm_in.nc')
    symlink.run(clm_source, clm_target)
    return clm_target


@task
def get_clm_bg_fname(end_time: pd.Timestamp) -> str:
    """
    This task is used to construct the file name of the CLM background files.

    Parameters
    ----------
    end_time : pd.Timestamp
        The name is constructed based on this timestamp.

    Returns
    -------
    bg_fname : str
        The constructed background file name.
    """
    timediff_clm = end_time - end_time.replace(hour=0, minute=0, second=0)
    timediff_clm_secs = timediff_clm.total_seconds()
    clm_file_name = 'clmoas.clm2.r.{0:s}-{1:05d}.nc'.format(
        end_time.strftime('%Y-%m-%d'), int(timediff_clm_secs)
    )
    return clm_file_name
