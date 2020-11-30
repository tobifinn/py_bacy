#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 6/18/19
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
import glob
import time

# External modules

# Internal modules
from .intf_pytassim import cosmo, clm
from .model import ModelModule
from .logger_mixin import LoggerMixin
from .utilities import check_if_folder_exist_create


logger = logging.getLogger(__name__)


class SymbolicModule(ModelModule, LoggerMixin):
    def run(self, start_time, analysis_time, parent_model, cycle_config):
        time.sleep(1)
        if self.config['COSMO']['analysis']:
            self.logger.info('Linking COSMO analysis')
            self.create_symbolic_cos(start_time, analysis_time, parent_model,
                                     cycle_config)
        if self.config['CLM']['analysis']:
            self.logger.info('Linking CLM analysis')
            self.create_symbolic_clm(start_time, analysis_time, parent_model,
                                     cycle_config)

    def get_dirs(self, start_time, analysis_time, parent_model, cycle_config):
        run_dir_current = self.get_run_dir(start_time, cycle_config)
        parent_analysis_dir = os.path.join(
            parent_model.get_run_dir(start_time, cycle_config), 'output'
        )
        dir_input = os.path.join(run_dir_current, 'input')
        dir_output = os.path.join(run_dir_current, 'output')
        dir_analysis = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            analysis_time.strftime('%Y%m%d_%H%M'),
            'analysis'
        )
        return parent_analysis_dir, dir_input, dir_output, dir_analysis

    def create_symbolic_cos(self, start_time, analysis_time, parent_model,
                            cycle_config):
        curr_dirs = self.get_dirs(start_time, analysis_time, parent_model,
                                  cycle_config)
        parent_analysis_dir, dir_input, dir_output, dir_analysis = curr_dirs
        bg_fname = cosmo.get_bg_filename(self.config['COSMO']['bg_files'], analysis_time)
        fname_analysis = analysis_time.strftime(cosmo.ANA_FNAME)
        bg_file_path = os.path.join(
            parent_analysis_dir, 'ens{0:03d}', bg_fname
        )
        for mem in range(1, cycle_config['ENSEMBLE']['size'] + 1):
            # Create folders
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_input_mem)
            dir_output_mem = os.path.join(dir_output, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_output_mem)
            analysis_dir_mem = os.path.join(
                dir_analysis, 'ens{0:03d}'.format(mem)
            )
            check_if_folder_exist_create(analysis_dir_mem)

            # Link background from parent to input
            bg_path_mem = bg_file_path.format(mem)
            self.logger.info('Member: {0:d}; Path: {1:s}'.format(
                mem, bg_path_mem
            ))
            bg_path_par = list(sorted(glob.glob(bg_path_mem)))
            self.logger.info('Member: {0:d}; Found bg files: {1:s}'.format(
                mem, ','.join(bg_path_par)
            ))
            wait_cnt = 0
            while not bg_path_par:
                self.logger.info('Found no file, I\'ll wait {0:d} s'.format(
                    2^wait_cnt
                ))
                time.sleep(2^wait_cnt)
                bg_path_par = list(sorted(glob.glob(bg_path_mem)))
                self.logger.info('Member: {0:d}; Found bg files: {1:s}'.format(
                    mem, ','.join(bg_path_par)
                ))
                wait_cnt += 1
                if wait_cnt > 5:
                    raise FileNotFoundError('Couldnt find any file!')
            bg_path_par = bg_path_par[0]
            bg_path_mem = os.path.join(dir_input_mem, bg_fname)
            self.logger.info('Copy to {0:s}'.format(bg_path_mem))
            self.symlink(bg_path_par, bg_path_mem)

            # Link analysis from input to output
            out_ana_path = os.path.join(dir_output_mem, fname_analysis)
            self.symlink(bg_path_mem, out_ana_path)

            # Link analysis from output to analysis
            out_ana_path = os.path.join(dir_output_mem, fname_analysis)
            ana_ana_path = os.path.join(analysis_dir_mem, fname_analysis)
            self.symlink(out_ana_path, ana_ana_path)

            self.logger.info('Finished COSMO member {0:d}'.format(mem))

    def create_symbolic_clm(self, start_time, analysis_time, parent_model,
                            cycle_config):
        curr_dirs = self.get_dirs(start_time, analysis_time, parent_model,
                                  cycle_config)
        parent_analysis_dir, dir_input, dir_output, dir_analysis = curr_dirs
        bg_fname = clm.get_bg_filename(self.config['CLM']['bg_files'], analysis_time)
        fname_analysis = analysis_time.strftime(clm.ANA_FNAME)
        bg_file_path = os.path.join(
            parent_analysis_dir, 'ens{0:03d}', bg_fname
        )
        for mem in range(1, cycle_config['ENSEMBLE']['size'] + 1):
            # Create folders
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_input_mem)
            dir_output_mem = os.path.join(dir_output, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_output_mem)
            analysis_dir_mem = os.path.join(
                dir_analysis, 'ens{0:03d}'.format(mem)
            )
            check_if_folder_exist_create(analysis_dir_mem)

            # Link background from parent to input
            bg_path_mem = bg_file_path.format(mem)
            bg_path_par = list(sorted(glob.glob(bg_path_mem)))[0]
            bg_path_mem = os.path.join(dir_input_mem, bg_fname)
            self.symlink(bg_path_par, bg_path_mem)

            # Link analysis from input to output
            out_ana_path = os.path.join(dir_output_mem, fname_analysis)
            self.symlink(bg_path_mem, out_ana_path)

            # Link analysis from output to analysis
            out_ana_path = os.path.join(dir_output_mem, fname_analysis)
            ana_ana_path = os.path.join(analysis_dir_mem, fname_analysis)
            self.symlink(out_ana_path, ana_ana_path)

            self.logger.info('Finished CLM member {0:d}'.format(mem))
