#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 12.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import logging

# External modules
from prefect import Flow, Parameter, unmapped, case, Task
from prefect.tasks.control_flow import merge
from prefect.tasks.core.function import FunctionTask
from prefect.tasks.core.constants import Constant

# Internal modules
from py_bacy.tasks.dask import *
from py_bacy.tasks.general import *
from py_bacy.tasks.system import *
from py_bacy.tasks.utils import *
from py_bacy.tasks.pytassim.utils import *


logger = logging.getLogger(__name__)


def get_pytassim_flow(
        link_background: Task,
        link_first_guess: Task,
        load_background: Task,
        load_first_guess: Task,
        load_obs: Task,
        initialize_assimilation: Task,
        post_process_analysis: Task,
        write_analysis: Task,
        link_analysis: Task,
):
    with Flow('pytassim') as pytassim_flow:
        start_time = Parameter('start_time')
        end_time = Parameter('end_time')
        analysis_time = Parameter('analysis_time')
        config_path = Parameter('config_path')
        cycle_config = Parameter('cycle_config')
        name = Parameter('name')
        use_fg = Parameter('use_fg', default=True)
        parent_model_name = Parameter('parent_model_name', default=None)
    
        pytassim_config = config_reader(config_path)
        run_dir = construct_rundir(
            name=name,
            time=start_time,
            cycle_config=cycle_config
        )
    
        ens_suffix, ens_range = construct_ensemble(cycle_config=cycle_config)
        zipped_directories = create_directory_structure.map(
            directories=unmapped(('input', 'output')),
            run_dir=unmapped(run_dir),
            ens_suffix=ens_suffix
        )
        input_dirs, output_dirs = unzip_mapped_result(
            zipped_directories, task_args=dict(nout=2)
        )
        parent_dirs = get_parent_output.map(
            cycle_config=unmapped(cycle_config),
            run_dir=unmapped(run_dir),
            ens_suffix=ens_suffix,
            parent_model_name=unmapped(parent_model_name),
        )
    
        cluster_mode = get_cluster_mode(cycle_config)
        with case(cluster_mode, 'slurm'):
            slurm_client, slurm_cluster = initialize_slurm_cluster(cycle_config)
        with case(cluster_mode, 'local'):
            local_client, local_cluster = initialize_local_cluster(cycle_config)
        with case(cluster_mode, None):
            no_client, no_cluster = None, None
        client = merge(slurm_client, local_client, no_client)
        cluster = merge(slurm_cluster, local_cluster, no_cluster)

        assimilation = initialize_assimilation(
            start_time=start_time,
            analysis_time=analysis_time,
            end_time=end_time,
            assim_config=pytassim_config,
            cycle_config=cycle_config,
            client=client
        )

        linked_bg_files = link_background.map(
            parent_model_output=parent_dirs,
            input_folder=input_dirs,
            config=unmapped(pytassim_config),
            cycle_config=unmapped(cycle_config),
            analysis_time=unmapped(analysis_time)
        )

        obs_window = get_observation_window(
            start_time=start_time,
            assim_config=pytassim_config,
            cycle_config=cycle_config
        )

        model_dataset, background = load_background(
            bg_files=linked_bg_files,
            analysis_time=analysis_time,
            assim_config=pytassim_config,
            cycle_config=cycle_config,
            ens_members=ens_range,
            client=client
        )

        observations = load_obs(
            obs_window=obs_window,
            assim_config=pytassim_config,
            cycle_config=cycle_config,
            client=client
        )

        with case(use_fg, True):
            linked_fg_files = link_first_guess.map(
                parent_model_output=parent_dirs,
                input_folder=input_dirs,
                config=pytassim_config,
                cycle_config=cycle_config,
                analysis_time=analysis_time
            )
            first_guess = load_first_guess(
                fg_files=linked_fg_files,
                obs_window=obs_window,
                assim_config=pytassim_config,
                cycle_config=cycle_config,
                ens_members=ens_range,
                client=client
            )
        first_guess = merge(first_guess, Constant(None))

        # obs_diagnostics = info_observations(
        #     first_guess=ds_fg,
        #     observations=ds_obs,
        #     run_dir=run_dir,
        #     client=client
        # )

        analysis = assimilate(
            assimilation=assimilation,
            background=background,
            observations=observations,
            first_guess=first_guess,
            analysis_time=analysis_time
        )

        analysis_dataset = post_process_analysis(
            analysis=analysis,
            model_dataset=model_dataset
        )

        written_analysis = write_analysis(
            analysis=analysis_dataset,
            background_files=linked_bg_files,
            output_dirs=output_dirs,
            analysis_time=analysis_time,
            assim_config=pytassim_config,
            cycle_config=cycle_config,
            client=client
        )

        # assimilation_diagnostics = info_assimilation(
        #     analysis=written_analysis,
        #     background=ds_bg,
        #     run_dir=run_dir,
        #     client=client
        # )

        analysis_dir = construct_rundir(
            name='analysis',
            time=analysis_time,
            cycle_config=cycle_config,
            upstream_tasks = [written_analysis]
        )
        path_join = FunctionTask(
            lambda prefix, suffix: os.path.join(prefix, suffix)
        )
        analysis_dirs = path_join.map(unmapped(analysis_dir), ens_suffix)
        analysis_dirs = create_folders.map(dir_path=analysis_dirs)
        linked_analysis = link_analysis.map(
            output_folder=output_dirs,
            analysis_folder=analysis_dirs,
            analysis_time=unmapped(analysis_time),
            upstream_tasks=[unmapped(written_analysis)]
        )

        shutdown_cluster(
            client=client,
            cluster=cluster,
            upstream_tasks=[linked_analysis]
        )
    return pytassim_flow
