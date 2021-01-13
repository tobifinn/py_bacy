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
from typing import List, Tuple

# External modules
from prefect import task

# Internal modules


__all__ = [
    'unzip_mapped_result'
]

@task
def unzip_mapped_result(
        mapped_result: List[Tuple]
) -> Tuple[List]:
    unzipped_result = tuple(zip(*mapped_result))
    return unzipped_result
