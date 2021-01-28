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
import os.path
from typing import Dict, Any, Union, List
import subprocess

# External modules
import prefect
from prefect import task, Flow
from prefect.engine.signals import LOOP

import pandas as pd

# Internal modules
from .system import copyfile


__all__ = [
    'readin_namelist_template',
    'modify_namelist_template',
    'write_namelist',
    'initialize_namelist',
    'link_binaries',
    'get_pids',
    'get_model_time_range',
    'loop_model_runs',
]


@task
def readin_namelist_template(model_config: Dict[str, Any]) -> str:
    """
    The template specified within the `template` keyword in the
    `model_config` is read-in as namelist template.
    The placeholders within this template have to be specified by
    %PLACEHOLDER_NAME%.
    The placeholders are model specific.

    Parameters
    ----------
    model_config : Dict[str, Any]
        The `template` keyword from this directory is read-in.

    Returns
    -------
    template : str
        The read-in namelist template.
    """
    template_path = model_config['template']
    with open(template_path, mode='r') as template_file:
        template = template_file.read()
    return template


@task
def modify_namelist_template(
        namelist_template: str,
        placeholder_dict: Dict[str, Any]
):
    """
    Placeholders in the form of %PLACEHOLDER_NAME% are replaced within the
    given namelist template.

    Parameters
    ----------
    namelist_template : str
        Placeholders within this template are replaced by the given value.
    placeholder_dict : Dict[str, Any]
        This is the directory with the placeholder values. A placeholder is
        skipped if it is not found within given namelist template. The values
        have to be castable to string type.

    Returns
    -------
    namelist_template : str
        The namelist where the placeholders are replaced with given values.
    """
    for placeholder, value in placeholder_dict.items():
        namelist_template = namelist_template.replace(placeholder, str(value))
    return namelist_template


@task
def write_namelist(target_folder: str, namelist_name: str, namelist: str):
    """
    This function writes a given namelist to a namelist path. The namelist
    path is constructed based on a given target folder and namelist name.
    The namelist automatically gets `755` as mode.

    Parameters
    ----------
    target_folder : str
        The namelist is written to this target folder. Normally, this is the
        input folder of a model run.
    namelist_name : str
        This is the namelist name under which the namelist should be stored.
    namelist : str
        This namelist is stored under the constructed target path.

    Returns
    -------
    target_path : str
        The constructed target path where the namelist is stored.
    """
    target_path = os.path.join(target_folder, namelist_name)
    with open(target_path, mode='w') as target_file:
        target_file.write(namelist)
        prefect.context.logger.debug(
            'Wrote the template to: {0:s}'.format(target_path)
        )
    subprocess.call(['chmod', '755', target_path])
    return target_path


@task
def initialize_namelist(
        namelist_path: str,
        ens_mem: int = 0
):
    """
    This function calls a given namelist path with given ensemble member as
    argument.

    Parameters
    ----------
    namelist_path : str
        This namelist is called.
    ens_mem : int
        This is the ensemble member which is used as argument for the
        namelist call.

    Returns
    -------
    namelist_path : str
        This called namelist path.
    """
    _ = subprocess.check_call([namelist_path, '{0:d}'.format(ens_mem)])
    return namelist_path


@task
def link_binaries(input_folder: str, model_config: Dict[str, Any]):
    """
    Link the binaries of the model into given input folder. All files within
    the specified directory are linked to the input folder.

    Parameters
    ----------
    input_folder : str
        The binaries are linked into this folder.
    model_config : Dict[str, Any]
        This cofiguration dictionary is used to determine with the `program`
        keyword the folder, where the binaries are stored.

    Returns
    -------
    linked_binaries : List[str]
        The paths to the linked binaries.
    """
    linked_binaries = []
    with os.scandir(model_config['program']) as bin_files:
        for bin_file in bin_files:
            source_path = bin_file.path
            file_name = os.path.basename(source_path)
            target_path = os.path.join(input_folder, file_name)
            copyfile.run(source_path, target_path)
            linked_binaries.append(target_path)
    return linked_binaries


@task
def get_pids(run_dir: str) -> List[str]:
    pid_path = os.path.join(run_dir, 'input', 'pid_file')
    with open(pid_path, mode='r') as pid_file:
        pid_strings = pid_file.read()
    pids = [pid for pid in pid_strings.split('\n') if pid != '']
    return pids


@task
def get_model_time_range(
        start_time: pd.Timestamp,
        end_time: pd.Timestamp,
        model_config: Dict[str, Any]
) -> List[pd.Timestamp]:
    logger = prefect.context.get('logger')
    restart_td = model_config['restart_td']
    model_steps = [pd.to_datetime(start_time), ]
    logger.debug('Got {0} as restart_td'.format(restart_td))
    for td in restart_td[:-1]:
        try:
            curr_td = pd.to_timedelta(td)
            new_step = model_steps[-1] + curr_td
        except ValueError:
            raise ValueError('Couldn\'t convert {0:s} into '
                             'timedelta'.format(td))
        model_steps.append(new_step)
    last_steps = pd.date_range(model_steps[-1], end_time,
                               freq=restart_td[-1])
    model_steps = model_steps + list(last_steps[1:])
    return model_steps


@task
def loop_model_runs(
        model_flow: Flow,
        name: str,
        model_steps: List[pd.Timestamp],
        parent_model_name: Union[str, None] = None,
        **kwargs
) -> str:
    logger = prefect.context.get('logger')

    loop_payload = prefect.context.get("task_loop_result", {})
    time_pos = loop_payload.get('time_pos', 0)
    curr_parent_name = loop_payload.get('parent_name', parent_model_name)
    curr_restart = loop_payload.get('restart', False)

    logger.debug('Current looped position: {0:d}'.format(time_pos))
    logger.debug('Current time: {0}'.format(model_steps[time_pos]))
    logger.debug('Waiting steps: {0}'.format(model_steps[time_pos:]))
    if time_pos < len(model_steps)-1:
        curr_start_time = model_steps[time_pos]
        curr_end_time = model_steps[time_pos+1]
        _ = model_flow.run(
            name=name,
            parent_model_name=curr_parent_name,
            restart=curr_restart,
            model_start_time=model_steps[time_pos],
            analysis_time=model_steps[time_pos+1],
            end_time=model_steps[time_pos+1],
            **kwargs
        )
        raise LOOP(
            message='Looped {0:s} for {1} -> {2}'.format(
                name, curr_start_time, curr_end_time
            ),
            result={
                'time_pos': time_pos + 1,
                'parent_name': name,
                'restart': True
            }
        )
    return name
