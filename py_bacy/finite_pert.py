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
import time
import os
import glob
from shutil import copyfile

# External modules
import xarray as xr
import numpy as np
import netCDF4 as nc

# Internal modules
from .intf_pytassim import cosmo, clm
from .utilities import check_if_folder_exist_create
from .assim_symbolic import SymbolicModule


logger = logging.getLogger(__name__)


class FinitePertModule(SymbolicModule):
    def run(self, start_time, analysis_time, parent_model, cycle_config):
        time.sleep(1)
        self.create_folders(start_time, analysis_time, parent_model,
                            cycle_config)
        self.create_symbolic_cos(start_time, analysis_time, parent_model,
                                 cycle_config)
        self.create_clm_perts(start_time, analysis_time, parent_model,
                              cycle_config)

    def create_folders(self, start_time, analysis_time, parent_model,
                       cycle_config):
        self.logger.info('Create in-/output folders')
        _, dir_input, dir_output, _ = self.get_dirs(
            start_time, analysis_time, parent_model, cycle_config
        )
        ens_mems = len(self.config['CLM']['pert']['levels']) + 2
        for mem in range(1, ens_mems):
            # Create folders
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_input_mem)
            dir_output_mem = os.path.join(dir_output, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_output_mem)
        self.logger.info('Created in-/output folders')

    def create_clm_perts(self, start_time, analysis_time, parent_model,
                         cycle_config):
        logger.info('Create CLM perturbations')
        curr_dirs = self.get_dirs(start_time, analysis_time, parent_model,
                                  cycle_config)
        parent_analysis_dir, dir_input, dir_output, dir_analysis = curr_dirs
        bg_fname = clm.get_bg_filename(self.config['CLM']['bg_files'],
                                       analysis_time)
        fname_analysis = analysis_time.strftime(clm.ANA_FNAME)

        const_data = clm.load_auxiliary_data(self.config['CLM']['utils_path'])

        # Assumption of deterministic run
        bg_file_path = os.path.join(
            parent_analysis_dir, 'ens001', bg_fname
        )
        pert_levels = [None, ] + list(self.config['CLM']['pert']['levels'])

        for mem, level in enumerate(pert_levels, 1):
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            dir_output_mem = os.path.join(dir_output, 'ens{0:03d}'.format(mem))

            # Link background from parent to input
            bg_path_mem = bg_file_path.format(mem)
            bg_path_par = list(sorted(glob.glob(bg_path_mem)))[0]
            bg_path_mem = os.path.join(dir_input_mem, bg_fname)
            self.symlink(bg_path_par, bg_path_mem)

            # Link analysis from input to output
            out_ana_path = os.path.join(dir_output_mem, fname_analysis)
            if level is None:
                self.symlink(bg_path_mem, out_ana_path)
            else:
                self._perturb_clm(bg_path_mem, out_ana_path, const_data, level,
                                  cycle_config)
            self.logger.info(
                'Created {0:d}th perturbations for level {1}'.format(
                    mem, level
                )
            )
        self.logger.info('Created CLM perturbations')

    def _perturb_clm(self, bg_path_mem, out_ana_path, const_data, level,
                     cycle_config):
        bg_ds = xr.open_dataset(bg_path_mem)
        h2o_liq = bg_ds['H2OSOI_LIQ'][:, 5:]
        delta_z = const_data['DZSOI'].T.values
        sat_point = const_data['WATSAT'].T.values
        h2o_vol = h2o_liq / clm.DENSITY / delta_z

        if self.config['CLM']['pert']['random']:
            rand_field = cycle_config['RandomState'].normal(
                scale=self.config['CLM']['pert']['scale'], size=h2o_vol.shape[0]
            )
        else:
            rand_field = self.config['CLM']['pert']['scale']
        h2o_vol[:, level] += rand_field
        wet_soi = h2o_vol / sat_point
        wet_soi = np.clip(wet_soi, 0, 1)
        h2o_liq_perturbed = wet_soi * sat_point * delta_z * clm.DENSITY
        copyfile(bg_path_mem, out_ana_path)
        with nc.Dataset(out_ana_path, mode='r+') as temp_ds:
            temp_ds['H2OSOI_LIQ'][:, 5:] = h2o_liq_perturbed.values

    def create_symbolic_cos(self, start_time, analysis_time, parent_model,
                            cycle_config):
        self.logger.info('Linking COSMO from previous output to our output')
        curr_dirs = self.get_dirs(start_time, analysis_time, parent_model,
                                  cycle_config)
        parent_analysis_dir, dir_input, dir_output, _ = curr_dirs
        bg_fname = cosmo.get_bg_filename(self.config['COSMO']['bg_files'],
                                         analysis_time)
        fname_analysis = analysis_time.strftime(cosmo.ANA_FNAME)

        # Assumption of deterministic run
        bg_file_path = os.path.join(
            parent_analysis_dir, 'ens001', bg_fname
        )
        ens_mems = len(self.config['CLM']['pert']['levels']) + 2
        for mem in range(1, ens_mems):
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            dir_output_mem = os.path.join(dir_output, 'ens{0:03d}'.format(mem))

            # Link background from parent to input
            bg_path_mem = bg_file_path.format(mem)
            bg_path_par = list(sorted(glob.glob(bg_path_mem)))
            wait_cnt = 0
            while not bg_path_par:
                self.logger.info('Found no file, I\'ll wait {0:d} s'.format(
                    2 ** wait_cnt
                ))
                time.sleep(2 ** wait_cnt)
                bg_path_par = list(sorted(glob.glob(bg_path_mem)))
                wait_cnt += 1
                if wait_cnt > 5:
                    raise FileNotFoundError('Couldnt find any file!')
            bg_path_par = bg_path_par[0]
            bg_path_mem = os.path.join(dir_input_mem, bg_fname)
            self.symlink(bg_path_par, bg_path_mem)

            # Link analysis from input to output
            out_ana_path = os.path.join(dir_output_mem, fname_analysis)
            self.symlink(bg_path_par, out_ana_path)
#
        self.logger.info('Linked COSMO output to output')