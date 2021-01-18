#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 18.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
from typing import Union, Any

# External modules
import prefect
from prefect import task

import xarray as xr

# Internal modules


@task
def constrain_var(
        dataset: xr.Dataset,
        constrain_var: str,
        lower_bound: Union[None, Any] = None,
        upper_bound: Union[None, Any] = None
) -> xr.Dataset:
    """
    This xarray utility task constrains a given variable within a given
    dataset to the given lower and upper bound.
    If the variable is not within the dataset, this constraining will be
    skipped.

    Parameters
    ----------
    dataset : xr.Dataset
        The variable specified in `constrain_var` will be constrained within
        a copy of this dataset.
    constrain_var : str
        This variable will be searched and constrained in the dataset.
        If the variable is not found, the constraining step will be skipped.
    lower_bound : None or array-like, optional
        This specifies the lower bound to which the variable will be
        contrained.
        If the default value of None is specified, there is no lower bound.
    upper_bound : None or array-like, optional
        This specifies the upper bound to which the variable will be
        contrained.
        If the default value of None is specified, there is no upper bound.

    Returns
    -------
    constrained_dataset : xr.Dataset
        The dataset with the constrained variable.
        If the variable cannot be found within the dataset, the original
        dataset will be returned.
    """
    logger = prefect.context.get('logger')
    try:
        constrained_dataset = dataset.copy()
        constrained_dataset[constrain_var] = dataset[constrain_var].clip(
            min=lower_bound, max=upper_bound
        )
    except KeyError:
        logger.debug(
            'Couldn\'t find {0:s} within the given dataset'.format(
                constrain_var
            )
        )
        constrained_dataset = dataset
    return constrained_dataset
