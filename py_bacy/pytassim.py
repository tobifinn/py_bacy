#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 5/23/19
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
import warnings
import abc
from typing import Any
import gc

# External modules
import numpy as np
import pandas as pd
import xarray as xr

from pytassim.assimilation.filter import DistributedLETKFUncorr
from pytassim.localization.gaspari_cohn import GaspariCohn

# Internal modules
from .logger_mixin import LoggerMixin
from .intf_pytassim import obs_op, utils
from .model import ModelModule
from .utilities import check_if_folder_exist_create


logger = logging.getLogger(__name__)


class PyTassimModule(ModelModule, LoggerMixin, abc.ABC):
    def __init__(self, name, parent=None, config=None):
        super().__init__(name, parent, config)
        self.assimilation = None

    @property
    @abc.abstractmethod
    def module(self):
        pass

    def init_assimilation(self, start_time, end_time, parent_model,
                          cycle_config):
        if 'smoother' not in self.config:
            self.config['smoother'] = True
        localization = GaspariCohn(
            np.array(self.config['loc_radius']),
            dist_func=self.module.distance_func
        )
        letkf = DistributedLETKFUncorr(
            client=cycle_config['CLUSTER']['client'],
            chunksize=self.config['chunksize'],
            localization=localization, inf_factor=self.config['inf_factor'],
            smoother=self.config['smoother']
        )
        return letkf

    def run(self, start_time, end_time, parent_model, cycle_config):
        self.assimilation = self.init_assimilation(
            start_time, end_time, parent_model, cycle_config
        )
        self.create_symbolic(start_time, end_time, parent_model,
                             cycle_config)
        self.assimilate_data(start_time, end_time, parent_model,
                             cycle_config)
        self.clean_up(start_time, end_time, parent_model, cycle_config)

    def clean_up(self, start_time, end_time, parent_model, cycle_config):
        del self.assimilation
        cycle_config['CLUSTER']['client'].restart()
        cycle_config['CLUSTER']['cluster'].scale(0)
        gc.collect()

    def disturb_obs(self, ds_obs: xr.Dataset) -> xr.Dataset:
        if not self.config['obs']['stochastic']:
            logger.info('No stochastic disturbance of observations')
            return ds_obs
        if ds_obs.obs.correlated:
            raise ValueError('Observations can be only disturbed for '
                             'uncorrelated observations!')
        ds_obs = ds_obs.copy()
        obs_stddev = np.sqrt(ds_obs['covariance'])
        drawn_noise = np.random.normal(
            scale=obs_stddev, size=ds_obs['observations'].shape
        )
        ds_obs['observations'] = ds_obs['observations'] + drawn_noise
        return ds_obs

    def localize_obs(
            self,
            ds_obs: xr.Dataset,
            analysis_time: Any
    ) -> xr.Dataset:
        if self.config['obs']['path_loc_mat'] is None:
            logger.info('No temporal localization of observations')
            return ds_obs
        ds_obs = ds_obs.copy()
        loc_mat = xr.open_dataset(self.config['obs']['path_loc_mat'])
        loc_mat = loc_mat['localization']
        sel_loc_mat = loc_mat.sel(analysis_time=analysis_time)
        sel_loc_mat['timedelta'] = sel_loc_mat['timedelta'] + pd.to_datetime(analysis_time)
        sel_loc_mat = sel_loc_mat.rename({'timedelta': 'time'})
        time_intersection = sel_loc_mat.indexes['time'].intersection(
            ds_obs.indexes['time']
        )
        sel_loc_mat = sel_loc_mat.sel(time=time_intersection)
        ds_obs = ds_obs.sel(time=time_intersection)
        non_zero_weight = sel_loc_mat != 0
        ds_obs = ds_obs.isel(time=non_zero_weight)
        sel_loc_mat = sel_loc_mat.isel(time=non_zero_weight)
        print('Temporal weights:', sel_loc_mat)
        cov_matrix = ds_obs['covariance'].expand_dims(time=ds_obs.time)
        cov_matrix = cov_matrix / sel_loc_mat
        ds_obs['covariance'] = cov_matrix
        logger.info('Observations are temporal localized')
        return ds_obs

    def assimilate_data(self, start_time, analysis_time, parent_model,
                        cycle_config):
        cycle_config['CLUSTER']['cluster'].scale(
            cycle_config['CLUSTER']['n_workers']
        )
        run_dir = self.get_run_dir(start_time, cycle_config)
        ensemble_members = cycle_config['ENSEMBLE']['size']
        util_dir = self.config['obs']['utils_path']
        file_path_obs = self.config['obs']['path']
        fg_files = self.config['obs']['fg_files']
        assim_vars = self.config['assim_vars']
        bg_files = self.config['bg_files']
        obs_timedelta = [
            pd.to_timedelta(self.config['obs']['td_start']),
            pd.to_timedelta(self.config['obs']['td_end']),
        ]
        lead_timedelta = pd.to_timedelta(
            cycle_config['TIME']['cycle_lead_time'], unit='S'
        )
        if obs_timedelta[-1] > lead_timedelta:
            warnings.warn(
                'The observation time delta is larger than the lead time delta,'
                'I will restrict the observation time delta to the lead time '
                'delta!', category=UserWarning
            )
            obs_timedelta[-1] = lead_timedelta

        ds_bg = self.module.load_background(
            run_dir, bg_files, analysis_time, ensemble_members,
            client=cycle_config['CLUSTER']['client']
        )

        ds_const = self.module.load_constant_data(util_dir)
        state_bg = self.module.preprocess_array(ds_bg, ds_const, analysis_time,
                                                assim_vars)
        obs_times = (start_time+obs_timedelta[0], start_time+obs_timedelta[1])
        state_fg, obs_raw, obs_operator = obs_op.load_obs_fg_t2m(
            run_dir, fg_files, ensemble_members, start_time, file_path_obs,
            util_dir, client=cycle_config['CLUSTER']['client']
        )

        logger.info('I\'ll slice the observations to {0}'.format(obs_times))
        state_fg = state_fg.sel(time=slice(obs_times[0], obs_times[1]))
        logger.info('First guess times: {0}'.format(
            state_fg.indexes['time']
        ))
        observations = obs_raw.sel(
            time=slice(obs_times[0], obs_times[1])
        )
        observations = self.disturb_obs(observations)
        observations = self.localize_obs(observations, analysis_time)
        observations.obs.operator = obs_operator
        logger.info('Observation times: {0}'.format(
            observations.indexes['time']
        ))
        utils.info_obs_diagonstics(state_fg, (observations, ), run_dir,
                                   self.name)

        state_analysis = self.assimilation.assimilate(
            state_bg, observations, state_fg
        ).compute()
        ds_ana = self.module.postprocess_array(state_analysis, ds_bg)
        ds_ana = self.module.correct_vars(ds_ana, ds_bg)
        utils.info_assimilation(ds_ana, ds_bg, assim_vars, run_dir, self.name)
        self.module.write_analysis(
            ds_ana, run_dir, bg_files, analysis_time, ensemble_members,
            assim_vars, client=cycle_config['CLUSTER']['client']
        )
        ds_bg.close()
        state_fg.close()
        ds_ana.close()
        del ds_bg
        del state_fg
        del ds_ana

    def create_symbolic(self, start_time, analysis_time, parent_model,
                        cycle_config):
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

        bg_fname = self.module.get_bg_filename(self.config['bg_files'],
                                               analysis_time)
        fname_analysis = analysis_time.strftime(self.module.ANA_FNAME)

        fg_file_path = os.path.join(parent_analysis_dir, 'ens{0:03d}',
                                    self.config['obs']['fg_files'])
        bg_file_path = os.path.join(
            parent_analysis_dir, 'ens{0:03d}', bg_fname
        )
        for mem in range(1, cycle_config['ENSEMBLE']['size'] + 1):
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_input_mem)
            dir_output_mem = os.path.join(dir_output, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_output_mem)
            analysis_dir_mem = os.path.join(
                dir_analysis, 'ens{0:03d}'.format(mem)
            )
            check_if_folder_exist_create(analysis_dir_mem)

            fg_files = glob.glob(fg_file_path.format(mem))
            for fpath in fg_files:
                fg_fname = os.path.basename(fpath)
                tmp_fg_path = os.path.join(dir_input_mem, fg_fname)
                self.symlink(fpath, tmp_fg_path)

            bg_path_mem = bg_file_path.format(mem)
            bg_path_found = list(sorted(glob.glob(bg_path_mem)))
            print(bg_path_found)
            bg_path_par = bg_path_found[0]
            bg_path_mem = os.path.join(dir_input_mem, bg_fname)
            self.symlink(bg_path_par, bg_path_mem)

            out_ana_path = os.path.join(dir_output_mem, fname_analysis)
            ana_ana_path = os.path.join(analysis_dir_mem, fname_analysis)
            self.symlink(out_ana_path, ana_ana_path)
