#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 05.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import logging

# External modules
from prefect import Flow, Parameter, unmapped
from prefect.tasks.control_flow.case import case

# Internal modules
from py_bacy.tasks.general import *
from py_bacy.tasks.model import *
from py_bacy.tasks.slurm import *
from py_bacy.tasks.system import *
from py_bacy.tasks.tsmp import *


logger = logging.getLogger(__name__)


with Flow('TerrSysMP model run') as tsmp_run:
    start_time = Parameter('start_time')
    end_time = Parameter('end_time')
    config_path = Parameter('config_path')
    cycle_config = Parameter('cycle_config')
    restart = Parameter('restart')
    name = Parameter('name')
    parent_output = Parameter('parent_output', default=None)

    config_reader = ReadInConfig()
    tsmp_config = config_reader(config_path)

    rundir_constructor = RundirConstructor()
    run_dir = rundir_constructor(name, start_time, cycle_config)

    create_replacement = CreateTSMPReplacement()
    placeholder_dict = create_replacement(
        start_time, end_time, run_dir, tsmp_config, cycle_config
    )

    namelist_modifier = ModifyNamelist()
    namelist = namelist_modifier(tsmp_config, placeholder_dict)

    ens_constructor = ConstructEnsemble()
    ens_suffix, ens_range = ens_constructor(cycle_config)

    folder_creator = CreateDirectoryStructure(
        directories=['input', 'output', 'analysis']
    )
    created_folders = folder_creator.map(unmapped(run_dir), ens_suffix)

    parent_getter = ParentGetter()
    parent_model_output = parent_getter.map(
        unmapped(cycle_config),
        unmapped(run_dir),
        ens_suffix,
        unmapped(parent_output)
    )

    data_linker = TSMPDataLinking()
    input_folders = data_linker.map(
        created_folders=created_folders,
        parent_model_output=parent_model_output,
        start_time=unmapped(start_time),
        tsmp_config=unmapped(tsmp_config),
        restart=unmapped(restart)
    )

    namelist_initializer = InitializeNamelist('tsmp_run.nml')
    execution_scripts = namelist_initializer.map(
        namelist=unmapped(namelist),
        input_folder=input_folders,
        mem=ens_range
    )

    slurm_executor = RunSlurmScript()
    output_folder = slurm_executor.map(
        execution_scripts, folders=created_folders
    )
    output_checker = CheckOutput([
        'clmoas.clm2.h0.*.nc',
        'clmoas.clm2.r.*.nc',
        'lffd*.nc_ana',
        'lfff*.nc_fg'
    ])
    flow_output = output_checker.map(output_folder)