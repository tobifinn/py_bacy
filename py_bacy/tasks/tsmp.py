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
from typing import Dict, Any
import datetime

# External modules
from prefect import Task

# Internal modules


class CreateReplacementDict(Task):
    def run(
            self,
            start_time: datetime.datetime,
            end_time: datetime.datetime,
            run_dir: str,
            tsmp_config: Dict[str, Any],
            cycle_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        model_start_time = start_time
        ini_time = datetime.datetime.strptime(
            cycle_config['TIME']['start_time'],
            cycle_config['TIME']['time_format'],
        )
        timedelta_start_ini = model_start_time - ini_time
        h_start = timedelta_start_ini.total_seconds() / 3600
        timedelta_seconds = int((end_time - model_start_time).total_seconds())
        timedelta_hours = timedelta_seconds / 3600
        analysis_seconds = int(cycle_config['TIME']['analysis_step'])
        if timedelta_seconds < analysis_seconds:
            analysis_seconds = timedelta_seconds
        clm_start_ymd = model_start_time.strftime('%Y%m%d')
        start_midnight = model_start_time.replace(hour=0, minute=0, second=0,
                                                  microsecond=0)
        clm_start_sec = int((model_start_time - start_midnight).total_seconds())
        clm_steps = (timedelta_seconds / tsmp_config['CLM']['dt']) + 1
        forcing_dir = tsmp_config['COSMO']['forcing']
        account = cycle_config['EXPERIMENT']['account']
        partition = cycle_config['EXPERIMENT']['partition']

        replacement_dict = {
            '%PROGRAM_DIR%': tsmp_config['program'],
            '%LOG_DIR%': tsmp_config['log_dir'],
            '%EXP_ID%': cycle_config['EXPERIMENT']['id'],
            '%H_START%': int(h_start),
            '%H_INT%': int(timedelta_hours),
            '%S_ANA%': analysis_seconds,
            '%N_RUNS%': tsmp_config['runs_per_job'],
            '%RUN_DIR%': run_dir,
            '%ACCOUNT%': account,
            '%PARTITION%': partition,

            '%COS_DATE_INI%': ini_time.strftime('%Y%m%d%H%M%S'),
            '%COS_DT%': tsmp_config['COSMO']['dt'],
            '%COS_FORC_DIR%': forcing_dir,

            '%CLM_DT%': tsmp_config['CLM']['dt'],
            '%CLM_OUT_TS%': tsmp_config['CLM']['out_ts'],
            '%CLM_START_YMD%': clm_start_ymd,
            '%CLM_START_SEC%': int(clm_start_sec),
            '%CLM_STEPS%': int(clm_steps),
        }
        return replacement_dict
