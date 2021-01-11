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
from typing import Dict, Any, Union
import datetime
import os
import glob
import tempfile

# External modules
from prefect import Task

# Internal modules
from py_bacy.intf_pytassim import cosmo, clm


__all__ = [
    'CreateTSMPReplacement',
    'TSMPDataLinking'
]


class CreateTSMPReplacement(Task):
    def run(
            self,
            name: str,
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
            '%NAME%': name,
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


class TSMPDataLinking(Task):
    def symlink(self, source: str, target: str) -> str:
        if not os.path.exists(source):
            raise ValueError(
                'Give source path {0:s} doesn\'t exists!'.format(
                    source
                )
            )
        self.logger.debug('Symlink: {0:s} -> {1:s}'.format(source, target))
        temp_name = next(tempfile._get_candidate_names())
        tmp_file = os.path.join(os.path.dirname(target), temp_name)
        os.symlink(source, tmp_file)
        os.replace(tmp_file, target)
        return target

    def link_binaries(
            self,
            created_folders: Dict[str, str],
            tsmp_config: Dict[str, Any]
    ):
        with os.scandir(tsmp_config['program']) as bin_files:
            for bin_file in bin_files:
                source_path = bin_file.path
                file_name = os.path.basename(source_path)
                target_path = os.path.join(created_folders['input'], file_name)
                self.symlink(source_path, target_path)

    def run(
            self,
            created_folders: Dict[str, str],
            parent_model_output: Union[None, str],
            start_time: datetime.datetime,
            tsmp_config: Dict[str, Any],
            restart: bool
    ) -> str:
        laf_file = start_time.strftime('laf%Y%m%d%H%M%S.nc')
        if restart:
            clm_out_file = clm.get_bg_filename(
                tsmp_config['CLM']['bg_files'], start_time
            )
            cos_out_files = cosmo.get_bg_filename(
                tsmp_config['COSMO']['bg_files'], start_time
            )
            cos_search_path = os.path.join(parent_model_output,
                                           cos_out_files)
            self.logger.info('COSMO search path: {0:s}'.format(cos_search_path))
            cos_source = list(sorted(glob.glob(cos_search_path)))[0]
        else:
            clm_out_file = start_time.strftime(
                'clm_ana%Y%m%d%H%M%S.nc'
            )
            cos_source = os.path.join(parent_model_output, laf_file)
        cos_target = os.path.join(created_folders['input'], laf_file)
        self.symlink(cos_source, cos_target)

        clm_source = os.path.join(parent_model_output, clm_out_file)
        clm_target = os.path.join(created_folders['input'], 'clm_in.nc')
        self.symlink(clm_source, clm_target)

        self.link_binaries(created_folders, tsmp_config)
        return created_folders['input']
