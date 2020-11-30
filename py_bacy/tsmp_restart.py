#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 21.02.20
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
from .tsmp import TSMPModule


logger = logging.getLogger(__name__)


class TSMPRestartModule(TSMPModule):
    @staticmethod
    def _get_timerange(start_time, end_time, restart_td):
        model_steps = [start_time, ]
        print('Got {0} as restart_td'.format(restart_td))
        for td in restart_td[:-1]:
            try:
                new_step = model_steps[-1] + pd.to_timedelta(td)
            except ValueError:
                raise ValueError('Couldn\'t convert {0:s} into '
                                 'timedelta'.format(td))
            model_steps.append(new_step)
        last_steps = pd.date_range(model_steps[-1], end_time,
                                   freq=restart_td[-1])
        model_steps = model_steps + list(last_steps[1:])
        return model_steps

    def run(self, start_time, end_time, parent_model, cycle_config,
            restart=False, ens_size=None, model_start_time=None):
        model_steps = self._get_timerange(
            start_time, end_time, self.config['restart_td']
        )
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
            super().run(start_time, curr_end_time,
                        curr_parent_model, cycle_config,
                        restart=curr_restart, ens_size=None,
                        model_start_time=curr_model_start_time)
