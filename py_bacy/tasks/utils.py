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
import logging
from typing import List, Tuple, Any

# External modules
from prefect import task

# Internal modules


__all__ = [
    'unzip_mapped_result',
    'slice_by_key'
]


@task
def unzip_mapped_result(
        mapped_result: List[Tuple]
) -> Tuple[List]:
    unzipped_result = tuple(zip(*mapped_result))
    return unzipped_result


@task
def slice_by_key(
        value_to_sliced: Any,
        key_tuple: Tuple[str]
) -> Any:
    sliced_value = value_to_sliced
    for level in key_tuple:
        sliced_value = sliced_value[level]
    return sliced_value
