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

# External modules
from prefect import Flow, Task

# Internal modules
from .pytassim_generic import get_pytassim_flow
from py_bacy.tasks.pytassim import clm, cosmo


logger = logging.getLogger(__name__)


def get_pytassim_clm(
        link_first_guess: Task,
        load_first_guess: Task,
        load_obs: Task,
        initialize_assimilation: Task
) -> Flow:
    pytassim_clm_flow = get_pytassim_flow(
        link_background=clm.link_background,
        link_first_guess=link_first_guess,
        load_background=clm.load_background,
        load_first_guess=load_first_guess,
        load_obs=load_obs,
        initialize_assimilation=initialize_assimilation,
        post_process_analysis=clm.post_process_analysis,
        write_analysis=clm.write_analysis,
        link_analysis=clm.link_analysis
    )
    return pytassim_clm_flow


def get_pytassim_cosmo(
        link_first_guess: Task,
        load_first_guess: Task,
        load_obs: Task,
        initialize_assimilation: Task
) -> Flow:
    pytassim_cos_flow = get_pytassim_flow(
        link_background=cosmo.link_background,
        link_first_guess=link_first_guess,
        load_background=cosmo.load_background,
        load_first_guess=load_first_guess,
        load_obs=load_obs,
        initialize_assimilation=initialize_assimilation,
        post_process_analysis=cosmo.post_process_analysis,
        write_analysis=cosmo.write_analysis,
        link_analysis=cosmo.link_analysis
    )
    return pytassim_cos_flow
