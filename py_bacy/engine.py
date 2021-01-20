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
from prefect import Flow
from prefect.engine.state import State

import pandas as pd

# Internal modules
from .tasks.general import config_reader


logger = logging.getLogger(__name__)


class CyclingEngine(object):
    def __init__(
            self,
            flow: Flow,
            cycle_config_path: str
    ):
        self._config = None
        self.flow = flow
        self.cycle_config_path = cycle_config_path

    @property
    def config(self):
        if self._config is None:
            self._config = config_reader.run(config_path=self.cycle_config_path)
        return self._config

    def run_single_time(
            self,
            start_time: pd.Timestamp,
            analysis_time: pd.Timestamp,
            end_time: pd.Timestamp
    ) -> State:
        flow_state = self.flow.run(
            start_time=start_time,
            analysis_time=analysis_time,
            end_time=end_time,
            cycle_config=self.config
        )
        return flow_state

    def start(self):
        time = pd.to_datetime(
            self.config['TIME']['start_time'],
            format=self.config['TIME']['time_format']
        )
        end_time = pd.to_datetime(
            self.config['TIME']['end_time'],
            format=self.config['TIME']['time_format']
        )
        analysis_timedelta = pd.to_timedelta(
            self.config['TIME']['analysis_step'],
            unit='seconds'
        )
        lead_timedelta = pd.to_timedelta(
            self.config['TIME']['cycle_lead_time'],
            unit='seconds'
        )

        while time < end_time:
            analysis_time = time + analysis_timedelta
            run_end_time = time + lead_timedelta
            logger.warning(
                'Starting with time {0:s}, analysis time {1:s} and '
                'run end time: {2:s}'.format(
                    time.strftime('%Y-%m-%d %H:%Mz'),
                    analysis_time.strftime('%Y-%m-%d %H:%Mz'),
                    run_end_time.strftime('%Y-%m-%d %H:%Mz'),
                )
            )
            flow_state = self.run_single_time(
                start_time=time,
                analysis_time=analysis_time,
                end_time=run_end_time
            )
            logger.warning(
                'Finished with time {0:s}, analysis time {1:s} and '
                'run end time: {2:s}'.format(
                    time.strftime('%Y-%m-%d %H:%Mz'),
                    analysis_time.strftime('%Y-%m-%d %H:%Mz'),
                    run_end_time.strftime('%Y-%m-%d %H:%Mz'),
                ))
            time = analysis_time
