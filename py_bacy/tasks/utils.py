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
from typing import Any

# External modules
from prefect import Flow, task

# Internal modules


logger = logging.getLogger(__name__)


@task
def run_external_flow(external_flow: Flow, **kwargs) -> Any:
    """
    This task runs a given external prefect.Flow with given additional
    keyword arguments.

    Parameters
    ----------
    external_flow : prefect.Flow
        This prefect flow is run with given additional keyword arguments.
    **kwargs
        These additional keyword arguments are passed to the run method of
        the given prefect run.

    Returns
    -------
    The output of flow's run method is returned.
    """
    return external_flow.run(**kwargs)
