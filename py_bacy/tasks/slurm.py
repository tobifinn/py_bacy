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
import subprocess
from typing import Dict, List
import time
import re

# External modules
import prefect
from prefect import task

import numpy as np

# Internal modules


__all__ = [
    'submit_script',
    'check_slurm_running'
]


@task
def submit_script(
        script_path: str
) -> str:
    call_args = ['sbatch', script_path]
    output = subprocess.run(call_args, capture_output=True)
    output_str = output.stdout.decode('utf-8')
    pid_str = re.findall(r'\b\d+\b', output_str)[0]
    return pid_str


@task
def check_slurm_running(
        pids: List[str],
        sleep_time: float = 5.0
) -> Dict[str, bool]:
        pids_str = ','.join(pids)
        pids_running = {pid: True for pid in pids}
        while any(pids_running.values()):
            time.sleep(sleep_time)
            squeue_output = subprocess.check_output(
                ['squeue', '--jobs={0:s}'.format(pids_str)], text=True
            )
            pids_running = {pid: pid in squeue_output for pid in pids}
            logger = prefect.context.get('logger')
            logger.info('Still runnning {0:d}/{1:d}'.format(
                int(np.sum(list(pids_running.values()))), len(pids_running)
            ))
            logger.debug(
                'Running PIDS: {0}'.format(pids_running)
            )
        return pids_running
