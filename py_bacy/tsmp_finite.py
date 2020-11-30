#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 12.06.20
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2020}  {Tobias Sebastian Finn}
#

# System modules
import logging

# External modules
import pandas as pd

# Internal modules
from .tsmp_restart import TSMPRestartModule


logger = logging.getLogger(__name__)


class TSMPFiniteModule(TSMPRestartModule):
    def run(self, start_time, end_time, parent_model, cycle_config,
            restart=False, ens_size=None, model_start_time=None):
        finite_start_time = end_time
        finite_end_time = end_time+pd.to_timedelta(self.config['end_td'])
        ens_size = self.config['ensemble_size']
        model_steps = self._get_timerange(finite_start_time,  finite_end_time,
                                          self.config['restart_td'])
        curr_parent_model = parent_model
        curr_restart = False
        for i, curr_model_start_time in enumerate(model_steps[:-1]):
            if i > 0:
                curr_restart = True
                curr_parent_model = self
            try:
                curr_end_time = model_steps[i+1]
            except KeyError:
                curr_end_time = end_time
            super(TSMPRestartModule, self).run(
                start_time, curr_end_time, curr_parent_model, cycle_config,
                restart=curr_restart, ens_size=ens_size,
                model_start_time=curr_model_start_time)
