#!/bin/env python
# -*- coding: utf-8 -*-
#
#Created on 27.07.17
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

# External modules
import xarray as xr
import numpy as np

# Internal modules
from .letkf import LetkfModule
from .utilities import check_if_folder_exist_create


logger = logging.getLogger(__name__)


fof_vars = {
    2: 'T',
    3: 'U',
    4: 'V',
    29: 'RH',
    156: 'HEIGHT'
}


class LetkfSingleModule(LetkfModule):
    def run(self, start_time, analysis_time, parent_model, cycle_config):
        if self.config['radar']:
            self.replacement_dict['%fl%'] = ''
        if self.config['radar_refl']:
            self.replacement_dict['%use_refl%'] = '11'
        if self.config['radar_vel']:
            self.replacement_dict['%use_radvel%'] = '11'
        self.replacement_dict['%EXP_ID%'] = cycle_config['EXPERIMENT']['id']
        self.replacement_dict['%N_ENS%'] = cycle_config['ENSEMBLE']['size']
        self.replacement_dict['%DET_RUN%'] = int(
            cycle_config['ENSEMBLE']['det'])
        self.replacement_dict['%DATE_ANA%'] = "{0:s}".format(
            analysis_time.strftime( '%Y%m%d%H%M%S'))
        self.replacement_dict['%DATE_FG%'] = "{0:s}".format(
            start_time.strftime( '%Y%m%d%H%M%S'))
        run_dir = self.get_run_dir(start_time, cycle_config)
        in_dir = os.path.join(
            run_dir,
            'input'
        )
        out_dir = os.path.join(
             run_dir,
            'output'
        )
        check_if_folder_exist_create(in_dir)
        check_if_folder_exist_create(out_dir)
        self.replacement_dict['%INPUT_DIR%'] = in_dir
        self.replacement_dict['%OUTPUT_DIR%'] = out_dir
        self.replacement_dict['%FG_FILE%'] = 'lff{0:s}.det'.format(
            start_time.strftime('%Y%m%d%H%M%S'))
        self.replacement_dict['%FG_ENS%'] = 'lff{0:s}'.format(
            start_time.strftime('%Y%m%d%H%M%S'))
        self.modify_fof(start_time, cycle_config, in_dir)
        self.create_symbolic(start_time, analysis_time, parent_model,
                             cycle_config, in_dir)
        templ_file = self.modify_namelist()
        self.execute_script(templ_file, cycle_config, in_dir)
        os.remove(templ_file)

    def modify_fof(self, start_time, cycle_config, in_dir):
        parent_dir = os.path.join(
            self.config['PARENT']['EXP_PATH'],
            start_time.strftime('%Y%m%d_%H%M'),
            self.config['PARENT']['MODEL'],
        )
        if cycle_config['ENSEMBLE']['det']:
            src_fof = os.path.join(
                parent_dir, 'output',
                'det',
                'fof_{0:s}.nc'.format(start_time.strftime('%Y%m%d%H%M%S'))
            )
            dst_fof = os.path.join(
                in_dir,
                os.path.basename(src_fof)
            )
            self._modify_single_fof(src_fof, dst_fof)
        for mem in range(1, cycle_config['ENSEMBLE']['size']+1):
            src_fof = os.path.join(
                parent_dir, 'output',
                'ens{0:03d}'.format(mem),
                'fof_{0:s}.nc'.format(start_time.strftime('%Y%m%d%H%M%S'))
            )
            dst_fof = os.path.join(
                in_dir,
                '{0:s}_ens{1:03d}.nc'.format(
                    os.path.splitext(os.path.basename(src_fof))[0], mem)
            )
            self._modify_single_fof(src_fof, dst_fof)

    def _modify_single_fof(self, src_fof, dst_fof):
            ds = xr.open_dataset(src_fof)
            logger.info('Opened dataset {0:s}'.format(
                src_fof
            ))
            unique_varno = np.unique(ds['varno'].dropna('d_body'))
            for num in unique_varno:
                var_name = fof_vars[int(num)]
                var_pos = ds['varno'] == num
                if 'exclude' in self.config.keys() and \
                        var_name in self.config['exclude']:
                    ds['state'][var_pos] = 7
                    ds['flags'][var_pos] = 32
                    ds['check'][var_pos] = 15
                    logger.info('Disable variable {0:s}'.format(var_name))
                else:
                    ds['state'][var_pos] = 1
                    ds['flags'][var_pos] = 0
                    ds['check'][var_pos] = 32
                    logger.info('Activate variable {0:s}'.format(var_name))
                    try:
                        bias_corr = float(self.config['bias_corr'][var_name])
                        ds['bcor'][var_pos] = ds['bcor'][var_pos] + bias_corr
                        ds['obs'][var_pos] = ds['obs'][var_pos] + bias_corr
                        logger.info(
                            'Bias correct variable {0:s} with {1:.2f}'.format(
                                var_name, bias_corr))
                    except KeyError:
                        pass
            ds.to_netcdf(dst_fof)
            logger.info('Saved the dataset to {0:s}'.format(dst_fof))

    def create_symbolic(self, start_time, analysis_time, parent_model,
                        cycle_config, in_dir):
        time_delta_ana_start = analysis_time-start_time
        time_delta_ana_start = datetime.datetime(1970,1,1)+time_delta_ana_start
        lff_target = 'lfff{0:02d}{1:s}'.format(
            time_delta_ana_start.day-1, time_delta_ana_start.strftime('%H%M%S'))
        parent_dir = os.path.join(
            self.config['PARENT']['EXP_PATH'],
            start_time.strftime('%Y%m%d_%H%M'),
            self.config['PARENT']['MODEL'],
        )
        if cycle_config['ENSEMBLE']['det']:
            src_lff = os.path.join(
                parent_dir, 'analysis',
                'det',
                lff_target
            )
            dst_lff = os.path.join(
                in_dir,
                'lff{0:s}.det'.format(start_time.strftime('%Y%m%d%H%M%S'))
            )
            self.symlink(src_lff, dst_lff)
            # src_fof = os.path.join(
            #     # parent_analysis_dir,
            #     parent_dir, 'output',
            #     'det',
            #     'fof_{0:s}.nc'.format(start_time.strftime('%Y%m%d%H%M%S'))
            # )
            # dst_fof = os.path.join(
            #     in_dir,
            #     os.path.basename(src_fof)
            # )
            # self.symlink(src_fof, dst_fof)
        for mem in range(1, cycle_config['ENSEMBLE']['size']+1):
            src_lff = os.path.join(
                parent_dir, 'analysis',
                'ens{0:03d}'.format(mem),
                lff_target
            )
            dst_lff = os.path.join(
                in_dir,
                'lff{0:s}.{1:03d}'.format(start_time.strftime('%Y%m%d%H%M%S'),
                                          mem)
            )
            self.symlink(src_lff, dst_lff)
            # src_fof = os.path.join(
            #     # parent_analysis_dir,
            #     parent_dir, 'output',
            #     'ens{0:03d}'.format(mem),
            #     'fof_{0:s}.nc'.format(start_time.strftime('%Y%m%d%H%M%S'))
            # )
            # dst_fof = os.path.join(
            #     in_dir,
            #     '{0:s}_ens{1:03d}.nc'.format(
            #         os.path.splitext(os.path.basename(src_fof))[0], mem)
            # )
            # self.symlink(src_fof, dst_fof)
