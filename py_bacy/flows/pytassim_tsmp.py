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
import logging
from typing import Union

# External modules
from prefect import Flow, Task

# Internal modules
from .pytassim_generic import get_pytassim_flow
from .symbolic import get_symbolic_flow
from py_bacy.tasks.pytassim import clm, cosmo


logger = logging.getLogger(__name__)


def get_pytassim_clm(
        link_first_guess: Task,
        load_first_guess: Task,
        load_obs: Task,
        initialize_assimilation: Task,
        post_process_obs: Union[Task, None] = None,
) -> Flow:
    pytassim_clm_flow = get_pytassim_flow(
        link_background=clm.link_background,
        link_first_guess=link_first_guess,
        load_background=clm.load_background,
        load_first_guess=load_first_guess,
        load_obs=load_obs,
        post_process_obs=post_process_obs,
        initialize_assimilation=initialize_assimilation,
        post_process_analysis=clm.post_process_analysis,
        write_analysis=clm.write_analysis,
    )
    return pytassim_clm_flow


def get_pytassim_cosmo(
        link_first_guess: Task,
        load_first_guess: Task,
        load_obs: Task,
        initialize_assimilation: Task,
        post_process_obs: Union[Task, None] = None,
) -> Flow:
    pytassim_cos_flow = get_pytassim_flow(
        link_background=cosmo.link_background,
        link_first_guess=link_first_guess,
        load_background=cosmo.load_background,
        load_first_guess=load_first_guess,
        load_obs=load_obs,
        post_process_obs=post_process_obs,
        initialize_assimilation=initialize_assimilation,
        post_process_analysis=cosmo.post_process_analysis,
        write_analysis=cosmo.write_analysis,
    )
    return pytassim_cos_flow


def get_symbolic_clm() -> Flow:
    symbolic_flow = get_symbolic_flow(
        link_background=clm.link_background,
        link_output=clm.link_output
    )
    return symbolic_flow


def get_symbolic_cosmo() -> Flow:
    symbolic_flow = get_symbolic_flow(
        link_background=cosmo.link_background,
        link_output=cosmo.link_output
    )
    return symbolic_flow
