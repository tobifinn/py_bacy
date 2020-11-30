#!/bin/env python
# -*- coding: utf-8 -*-
#
#Created on 08.03.17
#
#Created for py_bacy
#
#@author: Tobias Sebastian Finn, tobias.sebastian.finn@studium.uni-hamburg.de
#
#    Copyright (C) {2017}  {Tobias Sebastian Finn}
#

# System modules
import logging
import os
import datetime
import glob

# External modules

# Internal modules
from .utilities import check_if_folder_exist_create
from .forecast_model import ForecastModelModule


logger = logging.getLogger(__name__)


class CosmoModule(ForecastModelModule):
    def __init__(self, name, parent=None, config=None):
        super().__init__(name, parent, config)
        self.replacement_dict['%LHN%']='.{0:s}.'.format(
            str(self.config['lhn']).upper())
        self.replacement_dict['%VERIF%']='.{0:s}.'.format(
            str(self.config['verify']).upper())

    def run(self, start_time, end_time, parent_model, cycle_config):
        self.replacement_dict['%EXP_ID%']=cycle_config['EXPERIMENT']['id']
        self.replacement_dict['%N_ENS%'] = cycle_config['ENSEMBLE']['size']
        self.replacement_dict['%DATE_INI%'] = start_time.strftime(
            '%Y%m%d%H%M%S')
        self.replacement_dict['%DATE_BD%'] = start_time.strftime('%Y%m%d%H%M%S')
        timedelta_end_start = (end_time-start_time)
        self.replacement_dict['%H_INT%'] = \
            timedelta_end_start.total_seconds()/3600
        run_dir = self.get_run_dir(start_time, cycle_config)
        if cycle_config['ENSEMBLE']['det']:
            self.create_symbolic(start_time, end_time, parent_model,
                                 cycle_config, run_dir, 'det')
        for ens_run in range(1,cycle_config['ENSEMBLE']['size']+1):
            self.create_symbolic(start_time, end_time, parent_model,
                                 cycle_config, run_dir, ens_run)
        templ_file = self.modify_namelist()
        in_dir = os.path.join(run_dir, 'input')
        self.execute_script(templ_file, cycle_config, in_dir)
        os.remove(templ_file)

    def create_symbolic(self, start_time, end_time, parent_model, cycle_config,
                        run_dir, suffix):
        in_dir = os.path.join(run_dir, 'input')
        out_dir = os.path.join(run_dir, 'output')
        ana_dir = os.path.join(run_dir, 'analysis')
        self.replacement_dict['%INPUT_DIR%']=in_dir
        self.replacement_dict['%OUTPUT_DIR%']=out_dir
        self.replacement_dict['%ANALYSIS_DIR%']=ana_dir
        parent_model_analysis = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            start_time.strftime('%Y%m%d_%H%M'),
            parent_model.name,
            'analysis'
        )
        if isinstance(suffix, str):
            in_dir = os.path.join(in_dir, suffix)
            out_dir = os.path.join(out_dir, suffix)
            ana_dir = os.path.join(ana_dir, suffix)
            parent_model_analysis = os.path.join(parent_model_analysis, suffix)
        else:
            in_dir = os.path.join(in_dir, 'ens{0:03d}'.format(suffix))
            out_dir = os.path.join(out_dir, 'ens{0:03d}'.format(suffix))
            ana_dir = os.path.join(ana_dir, 'ens{0:03d}'.format(suffix))
            parent_model_analysis = os.path.join(parent_model_analysis,
                                                 'ens{0:03d}'.format(suffix))
        check_if_folder_exist_create(in_dir)
        check_if_folder_exist_create(out_dir)
        check_if_folder_exist_create(ana_dir)
        laf_file = 'laf{0:s}'.format(start_time.strftime('%Y%m%d%H%M%S'))
        source = os.path.join(parent_model_analysis, laf_file)
        target = os.path.join(in_dir,
                              'laf{0:s}'.format(
                                  start_time.strftime('%Y%m%d%H')))
        self.symlink(source, target)
        lbf_files = []
        time = start_time
        while time<=end_time:
            time_delta = time-start_time
            time_delta = datetime.datetime(1970,1,1)+time_delta
            lbf_file = 'lbff{0:02d}{1:s}'.format(time_delta.day-1,
                                                time_delta.strftime('%H%M%S'))
            lbf_files.append(lbf_file)
            time = time+datetime.timedelta(hours=1)
        for file in lbf_files:
            source = os.path.join(parent_model_analysis, file)
            target = os.path.join(in_dir, file)
            self.symlink(source, target)
        obs_files = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            start_time.strftime('%Y%m%d_%H%M'),
            'obs', '*')
        for file in glob.glob(obs_files):
            target = os.path.join(in_dir, os.path.basename(file))
            self.symlink(file, target)
