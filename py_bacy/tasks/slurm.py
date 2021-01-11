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
import subprocess
from typing import Dict, List
import os
import time

# External modules
from prefect import Task

# Internal modules


logger = logging.getLogger(__name__)


class CheckSlurmRuns(Task):
    def __init__(self, sleep_time_in_sec: float = 5.0, **kwargs):
        super().__init__(**kwargs)
        self.sleep_time = sleep_time_in_sec

    def get_pids(self, pid_path: str) -> List[str]:
        with open(pid_path, mode='r') as pid_file:
            pid_strings = pid_file.read()
        pids = [pid for pid in pid_strings.split('\n') if pid != '']
        return pids

    def check_heartbeat(self, pids: List[str]) -> bool:
        running = False
        pids_str = ','.join(pids)
        squeue_output = subprocess.check_output(
            ['squeue', '--jobs={0:s}'.format(pids_str)], text=True
        )
        pids_running = {pid: pid in squeue_output for pid in pids}
        self.logger.debug('Running PIDS: {0}'.format(pids_running))
        if any(pids_running.values()):
            running = True
        return running

    def run(self, run_dir: str, folders: List[Dict[str, str]]) -> List[str]:
        running = True
        while running:
            time.sleep(self.sleep_time)
            pid_path = os.path.join(run_dir, 'input', 'pid_file')
            pids = self.get_pids(pid_path)
            running = self.check_heartbeat(pids)
        output_folders = [folder_dict['output'] for folder_dict in folders]
        return output_folders
