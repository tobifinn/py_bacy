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
from typing import Dict, Any
import os.path
import glob

# External modules
import prefect
from prefect import task

import pandas as pd

# Internal modules
from .system import symlink


__all__ = [
    'link_cos_restart',
    'link_cos_initial',
    'get_cos_bg_fname'
]


@task
def link_cos_restart(
        parent_model_output: str,
        output_fname: str,
        input_folder: str,
        model_start_time: pd.Timestamp,
) -> str:
    """
    Link laf* files to start COSMO from a given output folder into the given
    input folder for the case of a internal model restart without any data
    assimilation.

    Parameters
    ----------
    parent_model_output : str
        This is the source folder where the laf-file is stored.
    output_fname : str
        Name of the laf-file within the source folder. Due to data
        assimilation, the name of the file might be different than laf*.
    input_folder : str
        The laf file is linked into this input folder.
    model_start_time : pd.Timestamp
        This is the model start time, which is used to construct the laf-file.

    Returns
    -------
    cos_target : str
        This is the target path, where the laf-file was linked to.
    """
    laf_file = model_start_time.strftime('laf%Y%m%d%H%M%S.nc')
    cos_search_path = os.path.join(parent_model_output, output_fname)
    prefect.context.logger.debug(
        'COSMO search path: {0:s}'.format(cos_search_path)
    )
    cos_source = list(sorted(glob.glob(cos_search_path)))[0]
    cos_target = os.path.join(input_folder, laf_file)
    symlink(cos_source, cos_target)
    return cos_target


@task
def link_cos_initial(
        input_folder: str,
        model_start_time: pd.Timestamp,
        parent_model_output: str
) -> str:
    """
    Link laf* files to start COSMO from a given output folder into the given
    input folder for the case of a fresh initial start of COSMO.

    Parameters
    ----------
    parent_model_output : str
        This is the source folder where the laf-file is stored.
    input_folder : str
        The laf file is linked into this input folder.
    model_start_time : pd.Timestamp
        This is the model start time, which is used to construct the laf-file.

    Returns
    -------
    cos_target : str
        This is the target path, where the laf-file was linked to.
    """
    laf_file = model_start_time.strftime('laf%Y%m%d%H%M%S.nc')
    cos_source = os.path.join(parent_model_output, laf_file)
    cos_target = os.path.join(input_folder, laf_file)
    symlink(cos_source, cos_target)
    return cos_target


@task
def get_cos_bg_fname(
        model_config: Dict[str, Any],
        analysis_time: pd.Timestamp
) -> str:
    """
    This task is used to construct the file name of the COSMO background files.

    Parameters
    ----------
    model_config : Dict[str, Any]
        This model configuration dictionary specifies the background file name
        under [COSMO][bg_files].
    analysis_time : pd.Timestamp
        The time of this timestamp is inserted into the bg_files regex.

    Returns
    -------
    bg_fname : str
        The constructed background file name.
    """
    bg_fname = analysis_time.strftime(model_config['COSMO']['bg_files'])
    return bg_fname
