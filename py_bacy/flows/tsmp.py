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
from prefect import Flow, Parameter, unmapped, context, case

# Internal modules
from py_bacy.tasks.clm import *
from py_bacy.tasks.cosmo import *
from py_bacy.tasks.general import *
from py_bacy.tasks.model import *
from py_bacy.tasks.slurm import *
from py_bacy.tasks.system import *
from py_bacy.tasks.tsmp import *


logger = logging.getLogger(__name__)

__all__ = [
    'get_tsmp_flow',
    'get_tsmp_restart_flow'
]


def get_tsmp_flow():
    with Flow('tsmp_run') as tsmp_run:
        start_time = Parameter('start_time')
        end_time = Parameter('end_time')
        config_path = Parameter('config_path')
        cycle_config = Parameter('cycle_config')
        restart = Parameter('restart', default=False)
        name = Parameter('name')
        parent_model_name = Parameter('parent_model_name', default=None)
        model_start_time = Parameter('model_start_time', default=None)

        if model_start_time is None:
            model_start_time = start_time
            context.logger.debug(
                'No model start time, will set the model start time to start '
                'time: {0}'.format(str(start_time))
            )

        tsmp_config = config_reader(config_path=config_path)
        run_dir = construct_rundir(
            name=name,
            time=start_time,
            cycle_config=cycle_config
        )

        ens_suffix, ens_range = construct_ensemble(cycle_config=cycle_config)
        input_dirs, output_dirs = create_input_output.map(
            run_dir=unmapped(run_dir),
            ens_suffix=ens_suffix
        )
        parent_dirs = get_parent_output.map(
            cycle_config=unmapped(cycle_config),
            run_dir=unmapped(run_dir),
            ens_suffix=ens_suffix,
            parent_model_name=unmapped(parent_model_name),
        )

        linked_binaries = link_binaries.map(
            input_folder=input_dirs,
            model_config=unmapped(tsmp_config)
        )
        with case(restart, True):
            clm_bg_fname = get_clm_bg_fname(end_time)
            cos_bg_fname = get_cos_bg_fname(tsmp_config, end_time)
            linked_clm_initial = link_clm_restart.map(
                parent_model_output=parent_dirs,
                output_fname=unmapped(clm_bg_fname),
                input_folder=input_dirs
            )
            linked_cos_initial = link_cos_restart.map(
                parent_model_output=parent_dirs,
                output_fname=unmapped(cos_bg_fname),
                input_folder=input_dirs,
                model_start_time=unmapped(model_start_time)
            )
        with case(restart, False):
            linked_clm_initial = link_clm_initial.map(
                parent_model_output=parent_dirs,
                model_start_time=unmapped(model_start_time),
                input_folder=input_dirs
            )
            linked_cos_initial = link_cos_initial.map(
                input_folder=input_dirs,
                parent_model_output=parent_dirs,
                model_start_time=unmapped(model_start_time),
            )

        placeholder_dict = create_tsmp_placeholders(
            name=name,
            model_start_time=model_start_time,
            end_time=end_time,
            run_dir=run_dir,
            tsmp_config=tsmp_config,
            cycle_config=cycle_config
        )
        namelist_template = readin_namelist_template(model_config=tsmp_config)
        modified_namelist = modify_namelist_template(
            namelist_template=namelist_template,
            placeholder_dict=placeholder_dict
        )
        namelist_paths = write_namelist.map(
            target_folder=input_dirs,
            namelist_name=unmapped('tsmp_run.nml'),
            namelist=unmapped(modified_namelist)
        )
        namelist_paths = initialize_namelist.map(
            namelist_path=namelist_paths, ens_mem=ens_range
        )

        slurm_pids = get_pids(
            run_dir=run_dir,
            upstream_tasks=[
                namelist_paths,
                linked_clm_initial,
                linked_cos_initial,
                linked_binaries
            ]
        )
        pids_running = check_slurm_running(pids=slurm_pids, sleep_time=5.0)
        output_dirs = check_output_files.map(
            output_folder=output_dirs,
            file_regex=unmapped([
                'clmoas.clm2.h0.*.nc',
                'clmoas.clm2.r.*.nc',
                'lffd*.nc_ana',
                'lfff*.nc_fg'
            ]),
            upstream_tasks=[pids_running]
        )
    return tsmp_run, output_dirs


def get_tsmp_restart_flow():
    tsmp_flow, _ = get_tsmp_flow()
    tsmp_restart_runner = RestartModelFlowRunner(tsmp_flow)
    with Flow('tsmp_restart_run') as restart_run:
        start_time = Parameter('start_time')
        end_time = Parameter('end_time')
        config_path = Parameter('config_path')
        cycle_config = Parameter('cycle_config')
        name = Parameter('name')
        parent_model_name = Parameter('parent_model_name', default=None)

        run_dir = construct_rundir(
            name=name,
            time=start_time,
            cycle_config=cycle_config
        )

        output_dirs = tsmp_restart_runner(
            name=name,
            start_time=start_time,
            end_time=end_time,
            run_dir=run_dir,
            config_path=config_path,
            cycle_config=cycle_config,
            parent_model_name=parent_model_name,
        )
    return restart_run, output_dirs
