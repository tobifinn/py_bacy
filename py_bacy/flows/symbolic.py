#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 19.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import logging

# External modules
from prefect import Flow, Task, Parameter, unmapped

# Internal modules
from py_bacy.tasks.dask import *
from py_bacy.tasks.general import *
from py_bacy.tasks.system import *
from py_bacy.tasks.utils import *
from py_bacy.tasks.pytassim.utils import *

logger = logging.getLogger(__name__)


def get_symbolic_flow(
        link_background: Task,
        link_output: Task,
) -> Flow:
    with Flow('symbolic') as symbolic_flow:
        start_time = Parameter('start_time')
        end_time = Parameter('end_time')
        analysis_time = Parameter('analysis_time')
        config_path = Parameter('config_path')
        cycle_config = Parameter('cycle_config')
        name = Parameter('name')
        parent_model_name = Parameter('parent_model_name', default=None)

        pytassim_config = config_reader(config_path)
        run_dir = construct_rundir(
            name=name,
            time=start_time,
            cycle_config=cycle_config
        )

        ens_suffix, ens_range = construct_ensemble(
            cycle_config=cycle_config
        )
        zipped_directories = create_directory_structure.map(
            directories=unmapped(('input', 'output')),
            run_dir=unmapped(run_dir),
            ens_suffix=ens_suffix
        )
        input_dirs, output_dirs = unzip_mapped_result(
            zipped_directories, task_args=dict(nout=2)
        )
        analysis_dirs = create_analysis_dir.map(
            cycle_config=unmapped(cycle_config),
            analysis_time=unmapped(analysis_time),
            ens_suffix=ens_suffix
        )
        parent_dirs = get_parent_output.map(
            cycle_config=unmapped(cycle_config),
            run_dir=unmapped(run_dir),
            ens_suffix=ens_suffix,
            parent_model_name=unmapped(parent_model_name),
        )

        linked_bg_files = link_background.map(
            parent_model_output=parent_dirs,
            input_folder=input_dirs,
            config=unmapped(pytassim_config),
            cycle_config=unmapped(cycle_config),
            analysis_time=unmapped(analysis_time)
        )
        linked_output_files = link_output.map(
            background_file=linked_bg_files,
            output_dir=output_dirs,
            analysis_time=unmapped(analysis_time)
        )
        linked_analysis_files = link_analysis.map(
            output_file=linked_output_files,
            analysis_folder=analysis_dirs
        )
    return symbolic_flow
