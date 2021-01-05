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

# External modules
from prefect import Task

# Internal modules


logger = logging.getLogger(__name__)


class RunSlurmScript(Task):
    def check_heartbeat(self, pid: int) -> bool:
        running = False
        squeue_output = subprocess.check_output(
            ['squeue', '--jobs={0:d}'.format(pid)], text=True
        )
        if str(pid) in squeue_output:
            running = True
        return running

    def run(self, path_script: str):
        slurm_return = subprocess.Popen([path_script], stdout=subprocess.PIPE)
        out, _ = slurm_return.communicate()
        pid = int(out.decode(encoding='UTF-8'))
        running = True
        while running:
            running = self.check_heartbeat(pid)
