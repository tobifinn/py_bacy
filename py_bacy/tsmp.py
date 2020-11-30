#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 4/11/19
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2019}  {Tobias Sebastian Finn}
#

# System modules
import logging
import os
import subprocess
import time
import datetime
import warnings
import glob

# External modules
import numpy as np
from tqdm import tqdm

from pympler import muppy, summary

# Internal modules
from .utilities import check_if_folder_exist_create
from .model import ModelModule
from .intf_pytassim import clm, cosmo


logger = logging.getLogger(__name__)


class TSMPModule(ModelModule):
    def __init__(self, name, parent=None, config=None):
        super().__init__(name, parent, config)

    def run(self, start_time, end_time, parent_model, cycle_config,
            restart=False, ens_size=None, model_start_time=None):
        self.replacement_dict['%EXP_ID%'] = cycle_config['EXPERIMENT']['id']
        ini_time = datetime.datetime.strptime(
            cycle_config['TIME']['start_time'],
            cycle_config['TIME']['time_format'],
        )
        self.replacement_dict['%COS_DATE_INI%'] = ini_time.strftime(
            '%Y%m%d%H%M%S'
        )
        if model_start_time is None:
            model_start_time = start_time
        timedelta_start_ini = model_start_time-ini_time
        h_start = timedelta_start_ini.total_seconds()/3600
        self.replacement_dict['%H_START%'] = int(h_start)
        timedelta_seconds = int((end_time-model_start_time).total_seconds())
        timedelta_hours = timedelta_seconds/3600
        self.replacement_dict['%H_INT%'] = int(timedelta_hours)
        analysis_seconds = int(cycle_config['TIME']['analysis_step'])
        if timedelta_seconds < analysis_seconds:
            analysis_seconds = timedelta_seconds
        self.replacement_dict['%S_ANA%'] = analysis_seconds
        self.replacement_dict['%N_RUNS%'] = self.config['runs_per_job']
        self.replacement_dict['%COS_DT%'] = self.config['COSMO']['dt']
        self.replacement_dict['%CLM_DT%'] = self.config['CLM']['dt']
        self.replacement_dict['%CLM_OUT_TS%'] = self.config['CLM']['out_ts']
        clm_start_ymd = model_start_time.strftime('%Y%m%d')
        self.replacement_dict['%CLM_START_YMD%'] = clm_start_ymd
        start_midnight = model_start_time.replace(hour=0, minute=0, second=0,
                                                  microsecond=0)
        clm_start_sec = int((model_start_time - start_midnight).total_seconds())
        self.replacement_dict['%CLM_START_SEC%'] = int(clm_start_sec)
        clm_steps = (timedelta_seconds / self.config['CLM']['dt']) + 1
        self.replacement_dict['%CLM_STEPS%'] = int(clm_steps)
        run_dir = self.get_run_dir(start_time, cycle_config)
        self.replacement_dict['%RUN_DIR%'] = run_dir
        forcing_dir = self.config['COSMO']['forcing']
        self.replacement_dict['%COS_FORC_DIR%'] = forcing_dir
        account = cycle_config['EXPERIMENT']['account']
        self.replacement_dict['%ACCOUNT%'] = account
        partition = cycle_config['EXPERIMENT']['partition']
        self.replacement_dict['%PARTITION%'] = partition

        if ens_size is None:
            ens_size = cycle_config['ENSEMBLE']['size']
        for ens_run in range(1, ens_size+1):
            self.create_symbolic(start_time, model_start_time, parent_model,
                                 cycle_config, run_dir, ens_run,
                                 restart=restart)
        templ_file = self.modify_namelist()
        in_dir = os.path.join(run_dir, 'input')

        self.execute_script(templ_file, cycle_config, in_dir, ens_size)
        os.remove(templ_file)

    def execute_script(self, script_path, cycle_config, in_dir, ens_size):
        ens_range = np.arange(1, ens_size+1, dtype=int)
        subprocess.call(['chmod', '755', script_path])
        running = True
        for mem in tqdm(ens_range):
            _ = subprocess.Popen([script_path, str(mem)])
            logger.debug(
                'Called the template for ensemble member: {0:d}'.format(mem)
            )
        while running:
            time.sleep(5)
            running = self.is_running(in_dir)
            if not running:
                logger.info('{0:s}: {1:s} is finished!'.format(
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M %S'),
                    self.name)
                )
            else:
                logger.info('{0:s}: {1:s} is running!'.format(
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M %S'),
                    self.name)
                )

    def create_symbolic(self, start_time, model_start_time,  parent_model,
                        cycle_config, run_dir, suffix, restart=False):
        if not isinstance(suffix, str):
            suffix = 'ens{0:03d}'.format(suffix)
        in_dir = os.path.join(run_dir, 'input', suffix)
        out_dir = os.path.join(run_dir, 'output', suffix)
        check_if_folder_exist_create(in_dir)
        check_if_folder_exist_create(out_dir)

        analysis_dir = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            start_time.strftime('%Y%m%d_%H%M'),
            'analysis',
            suffix
        )
        initial_dir = os.path.join(
            cycle_config['EXPERIMENT']['path_init'], suffix
        )
        parent_model_analysis = None
        if parent_model:
            parent_model_analysis = os.path.join(
                cycle_config['EXPERIMENT']['path'],
                start_time.strftime('%Y%m%d_%H%M'),
                parent_model.name,
                'output',
                suffix
            )
        elif os.path.isdir(analysis_dir):
            parent_model_analysis = analysis_dir
        elif os.path.isdir(initial_dir):
            parent_model_analysis = initial_dir
        else:
            warnings.warn('Cannot find any analysis directory for TSMP!',
                          UserWarning)
        logger.info('Initial dir: {0}'.format(parent_model_analysis))
        if parent_model_analysis is not None:
            laf_file = model_start_time.strftime('laf%Y%m%d%H%M%S.nc')
            if restart:
                clm_out_file = clm.get_bg_filename(
                    self.config['CLM']['bg_files'], model_start_time
                )
                cos_out_files = cosmo.get_bg_filename(
                    self.config['COSMO']['bg_files'], model_start_time
                )
                cos_search_path = os.path.join(parent_model_analysis,
                                               cos_out_files)
                logger.info('COSMO search path: {0:s}'.format(cos_search_path))
                cos_source = list(sorted(glob.glob(cos_search_path)))[0]
            else:
                clm_out_file = model_start_time.strftime(
                    'clm_ana%Y%m%d%H%M%S.nc'
                )
                cos_source = os.path.join(parent_model_analysis, laf_file)
            cos_target = os.path.join(in_dir, laf_file)
            self.symlink(cos_source, cos_target)

            clm_source = os.path.join(parent_model_analysis, clm_out_file)
            clm_target = os.path.join(in_dir, 'clm_in.nc')
            self.symlink(clm_source, clm_target)
        with os.scandir(self.config['program']) as bin_files:
            for bin_file in bin_files:
                source_path = bin_file.path
                file_name = os.path.basename(source_path)
                target_path = os.path.join(in_dir, file_name)
                self.symlink(source_path, target_path)
