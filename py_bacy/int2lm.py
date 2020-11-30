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
import subprocess

# External modules

# Internal modules
from .utilities import round_time
from .utilities import check_if_folder_exist_create
from .forecast_model import ForecastModelModule


logger = logging.getLogger(__name__)


class Int2lmModule(ForecastModelModule):
    def __init__(self, name, parent=None, config=None):
        super().__init__(name, parent, config)
        self.replacement_dict['%RB_DET%'] = self.config['RB_DET']
        self.replacement_dict['%RB_ENS%'] = self.config['RB_ENS']
        self.replacement_dict['%NRB_DET%'] = self.config['NRB_DET']
        self.replacement_dict['%NRB_ENS%'] = self.config['NRB_ENS']

    def run(self, start_time, end_time, parent_model, cycle_config):
        self.replacement_dict['%N_ENS%'] = cycle_config['ENSEMBLE']['size']
        self.replacement_dict['%DATE%'] = start_time.strftime('%Y%m%d%H%M%S')
        timedelta_end_start = (end_time-start_time)
        self.replacement_dict['%HSTOP%'] = \
            timedelta_end_start.total_seconds()/3600
        run_dir = self.get_run_dir(start_time, cycle_config)
        analysis_dir = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            start_time.strftime('%Y%m%d_%H%M'),
            'analysis'
        )
        if os.path.isdir(analysis_dir):
            self.config['int_analysis'] = False
            self.replacement_dict['%LINITIAL%']='.FALSE.'
        else:
            self.config['int_analysis'] = True
            self.replacement_dict['%LINITIAL%']='.TRUE.'
        if cycle_config['ENSEMBLE']['det']:
            self.create_symbolic(start_time, end_time, parent_model,
                                 cycle_config, run_dir, 'det', analysis_dir)
        for ens_run in range(1,cycle_config['ENSEMBLE']['size']+1):
            self.create_symbolic(start_time, end_time, parent_model,
                                 cycle_config, run_dir, ens_run, analysis_dir)
        self.symlink(os.path.join(run_dir, 'output'),
                   os.path.join(run_dir, 'analysis'), target_is_directory=True)
        templ_file = self.modify_namelist()
        in_file = os.path.join(
            run_dir,
            'input'
        )
        self.execute_script(templ_file, cycle_config, in_file)
        os.remove(templ_file)

    def create_symbolic(self, start_time, end_time, parent_model, cycle_config,
                        run_dir, suffix, analysis_dir):
        in_dir = os.path.join(run_dir, 'input')
        out_dir = os.path.join(run_dir, 'output')
        self.replacement_dict['%INPUT_DIR%']=in_dir
        self.replacement_dict['%OUTPUT_DIR%']=out_dir
        if isinstance(suffix, str):
            in_dir = os.path.join(in_dir, suffix)
            out_dir = os.path.join(out_dir, suffix)
            NRB = self.config['NRB_DET']
            RB = self.config['RB_DET']
            const_suffix = 'det'
        else:
            in_dir = os.path.join(in_dir, 'ens{0:03d}'.format(suffix))
            out_dir = os.path.join(out_dir, 'ens{0:03d}'.format(suffix))
            NRB = self.config['NRB_ENS']
            RB = self.config['RB_ENS']
            const_suffix = 'ens'
        check_if_folder_exist_create(in_dir)
        check_if_folder_exist_create(out_dir)
        if parent_model is None:
            td_hours = int((end_time-start_time).total_seconds()/3600)
            date_range = [start_time+datetime.timedelta(hours=h)
                          for h in range(td_hours+1)]
            for date in date_range:
                self.create_link(date_range[0], date, in_dir, suffix,
                                 self.config['boundary_path'])
        else:
            parent_model_analysis = os.path.join(
                cycle_config['EXPERIMENT']['path'],
                start_time.strftime('%Y%m%d_%H%M'),
                parent_model.name,
                'analysis'
            )
            analysis_files = [os.path.join(parent_model_analysis, file)
                              for file in os.listdir(parent_model_analysis)
                              if os.path.isfile(
                    os.path.join(parent_model_analysis, file))]
            for file in analysis_files:
                self.symlink(file, os.path.join(in_dir, os.path.basename(file)))
        src_grid = os.path.join(
            self.config['coarse_extpar_dir'],
            'icon_grid_{0:s}_{1:s}_N02.nc'.format(NRB, RB)
        )
        target_grid = os.path.join(in_dir, os.path.basename(src_grid))
        self.symlink(
            src_grid,
            target_grid
        )
        src_extpar = os.path.join(
            self.config['coarse_extpar_dir'],
            'icon_extpar_{0:s}_{1:s}_N02_20150805.nc'.format(NRB, RB)
        )
        self.symlink(
            src_extpar,
            os.path.join(in_dir, os.path.basename(src_extpar))
        )
        src_hhl_file = os.path.join(
            self.config['coarse_extpar_dir'],
            'COSMO_HH_HHL_002_051_001.grb2'
        )
        self.symlink(
            src_hhl_file,
            os.path.join(in_dir, os.path.basename(src_hhl_file))
        )
        self.symlink(
            self.config['cosmo_extpar'],
            os.path.join(in_dir, os.path.basename(self.config['cosmo_extpar']))
        )
        self.replacement_dict['%EXTPAR_COSMO%'] = os.path.basename(
            self.config['cosmo_extpar'])

        self.symlink(
            self.config['const_path']+'_{0:s}'.format(const_suffix),
            os.path.join(in_dir, 'iefff00000000c')
        )
        self.replacement_dict['%CONST_COARSE%'] = 'iefff00000000c'
        if not self.config['int_analysis']:
            if isinstance(suffix, int):
                analysis_suffix = '{0:03d}'.format(suffix)
            else:
                analysis_suffix = 'det'
            analysis_file = os.path.join(
                analysis_dir,
                'laf{0:s}.{1:s}'.format(start_time.strftime('%Y%m%d%H%M%S'),
                                        analysis_suffix))
            self.symlink(
                analysis_file,
                os.path.join(
                    out_dir,
                    os.path.splitext(os.path.basename(analysis_file))[0])
            )

    def create_link(self, start_time, time, in_dir, suffix, data_path):
        date, time_delta = round_time(time, 12*3600)
        date = date.strftime('%Y%m%d%H')
        time_delta = time_delta+datetime.datetime(1970,1,1)
        if isinstance(suffix, int):
            ln_suffix = '.m{0:03d}'.format(suffix)
        else:
            ln_suffix = ''
        source_path = os.path.join(
            data_path,
            date,
            'FC',
            'iefff{0:02d}{1:s}{2:s}'.format(
                time_delta.day-1, time_delta.strftime('%H%M%S'), ln_suffix)
        )
        target_time_delta = (time-start_time)+datetime.datetime(1970,1,1)
        target_path = os.path.join(
            in_dir,
            'iefff{0:02d}{1:s}'.format(target_time_delta.day-1,
                                       target_time_delta.strftime('%H%M%S'))
        )
        try:
            self.symlink(source_path, target_path)
        except FileExistsError:
            logger.debug('The target path already exists'.format(target_path))
