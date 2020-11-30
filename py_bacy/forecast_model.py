#!/bin/env python
# -*- coding: utf-8 -*-
#
#Created on 21.03.17
#
#Created for py_bacy
#
#@author: Tobias Sebastian Finn, tobias.sebastian.finn@studium.uni-hamburg.de
#
#    Copyright (C) {2017}  {Tobias Sebastian Finn}
#

# System modules
import logging
import subprocess
import time
import os
import datetime

# External modules
import numpy as np

# Internal modules
from .model import ModelModule


logger = logging.getLogger(__name__)


class ForecastModelModule(ModelModule):
    def execute_script(self, script_path, cycle_config, in_dir):
        ens_bounds = np.linspace(
            1,
            cycle_config['ENSEMBLE']['size']+1,
            cycle_config['ENSEMBLE']['parallel_executions']+1,
            dtype=int
        )
        call_appended_det = cycle_config['ENSEMBLE']['det'] and not \
                cycle_config['ENSEMBLE']['det_parallel']
        if call_appended_det:
            det_list = ['true']+['false']*(
                cycle_config['ENSEMBLE']['parallel_executions']-1)
        else:
            det_list = ['false']*cycle_config['ENSEMBLE']['parallel_executions']
        for i in range(0, len(ens_bounds)-1):
            subprocess.call(['chmod', '755', script_path])
            p = subprocess.Popen([script_path,
                                  det_list[i],
                                  str(ens_bounds[i]),
                                  str(ens_bounds[i+1]-1)])
        call_parallel_det = cycle_config['ENSEMBLE']['det'] and \
                    cycle_config['ENSEMBLE']['det_parallel']
        if call_parallel_det:
            subprocess.call(['chmod', '755', script_path])
            p = subprocess.Popen([script_path,
                                  'true',
                                  '1',
                                  '0'])
            logger.debug('Called the template for det run')
        running = True
        while running:
            time.sleep(1)
            running = self.is_running(in_dir)
            if not running:
                logger.info('{0:s}: {1:s} is finished!'.format(
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M %S'),
                    self.name)
                )
