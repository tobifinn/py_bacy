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
import pickle as pk
import os
import glob
from shutil import copyfile

# External modules
import pandas as pd
import xarray as xr
import numpy as np
import torch
import scipy.spatial
import netCDF4 as nc4
from tabulate import tabulate

from pytassim.assimilation import LETKFUncorr
from pytassim.assimilation.filter.letkf import local_etkf
from pytassim.localization import GaspariCohn
from pytassim.obs_ops.terrsysmp.cos_t2m import CosmoT2mOperator, EARTH_RADIUS
from pytassim.model.terrsysmp.cosmo import preprocess_cosmo, postprocess_cosmo
from pytassim.model.terrsysmp.clm import preprocess_clm, postprocess_clm

# Internal modules
from .logger_mixin import LoggerMixin
from .model import ModelModule


_height_vars = ['level', 'level1', 'levlak', 'levsno', 'levtot', 'numrad',
                'height_2m', 'height_10m', 'height_toa', 'soil1']


def cart_dist_func(x, y):
    deg_m = 2 * np.pi * EARTH_RADIUS / 360
    x_arr = np.atleast_1d(
        np.array(x, dtype=('float, float'))
    ).view(float).reshape(-1, 2)
    y_arr = np.atleast_1d(
        np.array(y, dtype=('float, float, float'))
    ).view(float).reshape(-1, 3)[..., :-1]
    diff = x_arr - y_arr
    hori_diff = diff * deg_m
    hori_diff = np.sqrt(np.sum(hori_diff ** 2, axis=-1))
    # vert_diff = np.abs(diff[..., -1])
    return hori_diff


def abs_dist_func(x, y):
    return np.abs(x-y)


class PytassimModule(ModelModule, LoggerMixin):
    def __init__(self, name, parent=None, config=None):
        super().__init__(name, parent, config)
        self.algorithm = self.init_assimilation()

    def init_assimilation(self):
        localization = GaspariCohn(np.array(self.config['loc_radius']),
                                   cart_dist_func)
        letkf = LETKFUncorr(localization=localization,
                            inf_factor=self.config['inf_factor'])
        return letkf

    def run(self, start_time, analysis_time, parent_model, cycle_config):
        self.create_symbolic(start_time, analysis_time, parent_model,
                             cycle_config)
        weights_assim = self.get_weights(start_time, cycle_config)
        self.assimilate_cosmo(weights_assim, start_time, analysis_time,
                              cycle_config)
        self.assimilate_clm(weights_assim, start_time, analysis_time,
                            cycle_config)

    def create_symbolic(self, start_time, analysis_time, parent_model,
                        cycle_config):
        run_dir_current = self.get_run_dir(start_time, cycle_config)
        os.makedirs(run_dir_current)
        parent_analysis_dir = os.path.join(
            parent_model.get_run_dir(start_time, cycle_config), 'output'
        )
        dir_input = os.path.join(run_dir_current, 'input')
        dir_output = os.path.join(run_dir_current, 'output')
        self.symlink(parent_analysis_dir, dir_input)
        for mem in range(1, cycle_config['ENSEMBLE']['size']+1):
            dir_output_mem = os.path.join(dir_output, 'ens{0:03d}'.format(mem))
            os.makedirs(dir_output_mem)
        run_dir_analysis = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            analysis_time.strftime('%Y%m%d_%H%M'),
        )
        try:
            os.makedirs(run_dir_analysis)
        except FileExistsError:
            pass
        dir_analysis = os.path.join(run_dir_analysis, 'analysis')
        self.symlink(dir_output, dir_analysis)

    def get_weights(self, start_time, cycle_config):
        ds_first_guess = self.get_first_guess(start_time, cycle_config)
        ds_obs = self.get_observations(cycle_config)
        df_stations = self.get_stations(cycle_config)
        ds_const_data = self.get_constant_data(cycle_config)
        coords_fg = self.get_coords('cosmo_grid.pk', cycle_config)

        prepared_state, prepared_obs = self.prepare_assimilation(
            ds_first_guess, ds_obs, df_stations, ds_const_data, coords_fg
        )
        weights_gridded = self.gen_weights(prepared_state, prepared_obs)
        weights_renamed = weights_gridded.rename({'ensemble_1': 'ensemble'})
        self.write_weights(weights_gridded, start_time, cycle_config)
        self.logger.info('Created weights for assimilation')
        return weights_renamed

    def assimilate_cosmo(self, weights, start_time, end_time, cycle_config):
        ds_cos_bg = self.get_cosmo_background(start_time, cycle_config)
        ds_cos_ana = self.apply_weights_surf_cos(
            ds_cos_bg, weights, self.config['COSMO']['assim_vars']
        )
        ds_cos_ana = self.correct_vars_cos(ds_cos_ana, ds_cos_bg)
        self.info_assimilation(
            ds_cos_ana, ds_cos_bg, start_time, cycle_config,
            self.config['COSMO']['assim_vars'],
        )
        self.write_cosmo(ds_cos_ana, start_time, end_time, cycle_config)

    def assimilate_clm(self, weights, start_time, end_time, cycle_config):
        coords_weights = self.get_coords('cosmo_grid.pk', cycle_config)
        coords_clm = self.get_coords('clm_grid.pk', cycle_config)

        ds_clm_bg = self.get_clm_background(start_time, end_time, cycle_config)
        weights_clm = self.interpolate_weights(
            coords_weights, coords_clm, weights
        )
        weights_clm = weights_clm.rename({'grid': 'column'})
        weights_clm['column'] = ds_clm_bg['column']
        ds_clm_ana = self.apply_weights_surf_clm(
            ds_clm_bg, weights_clm, self.config['CLM']['assim_vars']
        )
        ds_clm_ana = self.correct_vars_clm(ds_clm_ana, ds_clm_bg)
        self.info_assimilation(
            ds_clm_ana, ds_clm_bg, start_time, cycle_config,
            self.config['CLM']['assim_vars']
        )
        self.write_clm(ds_clm_ana, start_time, end_time, cycle_config)

    def write_weights(self, ds_weights, start_time, cycle_config):
        run_dir = self.get_run_dir(start_time, cycle_config)
        out_dir = os.path.join(run_dir, 'output')
        file_path_weights = os.path.join(out_dir, 'ens_weights.nc')
        ds_weights.to_netcdf(file_path_weights)

    @staticmethod
    def prepare_assimilation(ds_first_guess, ds_obs, df_stations,
                             ds_const_data, coords_fg):
        obs_operator = CosmoT2mOperator(
            df_stations, coords_fg, ds_const_data
        )
        prepared_obs = ds_obs.sel(time=ds_first_guess.time.values)
        prepared_obs.obs.operator = obs_operator
        prepared_state = preprocess_cosmo(ds_first_guess, ['T', 'T_2M']).load()
        return prepared_state, prepared_obs

    def get_coords(self, coords_name, cycle_config):
        util_dir = self.expand_dir(self.config['utils_path'], cycle_config)
        file_path_coords = os.path.join(util_dir, coords_name)
        coords_latlon = np.stack(
            pk.load(open(file_path_coords, 'rb'))._calc_lat_lon(), axis=-1
        )
        self.logger.info(
            'Loaded grid coords from {0:s}'.format(file_path_coords)
        )
        return coords_latlon

    def get_constant_data(self, cycle_config):
        util_dir = self.expand_dir(self.config['utils_path'], cycle_config)
        file_path_const = os.path.join(util_dir, 'cosmo_const.nc')
        ds_cos_const = xr.open_dataset(file_path_const).load()
        self.logger.info(
            'Loaded constant COSMO data from {0:s}'.format(file_path_const)
        )
        return ds_cos_const

    def get_stations(self, cycle_config):
        util_dir = self.expand_dir(self.config['utils_path'], cycle_config)
        file_path_station = os.path.join(util_dir, 'stations.hd5')
        df_stations = pd.read_hdf(file_path_station, 'stations')
        self.logger.info(
            'Loaded station informations from {0:s}'.format(file_path_station)
        )
        return df_stations

    def get_first_guess(self, start_time, cycle_config):
        run_dir = self.get_run_dir(start_time, cycle_config)
        in_dir = os.path.join(
            run_dir,
            'input'
        )
        file_path_first_guess = os.path.join(
            in_dir, 'ens{0:03d}', self.config['fg_files']
        )
        ds_first_guess = self.load_ens_data(
            file_path=file_path_first_guess,
            ens_mems=cycle_config['ENSEMBLE']['size']
        )
        after_start = ds_first_guess.indexes['time'] > start_time.isoformat()
        ds_first_guess = ds_first_guess.sel(time=after_start)
        self.logger.info('Loaded first guess data')
        return ds_first_guess

    def get_observations(self, cycle_config):
        file_path_obs = self.expand_dir(self.config['obs_path'], cycle_config)
        ds_obs_raw = xr.open_dataset(file_path_obs).load()
        ds_obs = ds_obs_raw.drop(ds_obs_raw.attrs['multiindex'])
        ds_obs['obs_grid_1'] = pd.MultiIndex.from_frame(
            ds_obs_raw[ds_obs_raw.attrs['multiindex']].to_dataframe()[
                ds_obs_raw.attrs['multiindex']])
        ds_obs.attrs = []
        self.logger.info('Loaded observations from {0:s}'.format(file_path_obs))
        return ds_obs

    def get_cosmo_background(self, start_time, cycle_config):
        run_dir = self.get_run_dir(start_time, cycle_config)
        in_dir = os.path.join(
            run_dir,
            'input'
        )
        file_path_bg = os.path.join(
            in_dir, 'ens{0:03d}', self.config['COSMO']['bg_files']
        )
        ds_cos_bg = self.load_ens_data(
            file_path=file_path_bg, ens_mems=cycle_config['ENSEMBLE']['size']
        ).load()
        return ds_cos_bg

    @staticmethod
    def apply_weights_surf_cos(ds_cosmo, weights, assim_vars):
        if assim_vars:
            bg_array = preprocess_cosmo(ds_cosmo, assim_vars).load()
            bg_array = bg_array.unstack('grid')
            bg_mean, bg_perts = bg_array.state.split_mean_perts()
            ana_perts = bg_perts.dot(weights, dims=['ensemble'])
            ana_array = bg_mean + ana_perts
            ana_array = ana_array.rename({'ensemble_2': 'ensemble'})
            ana_array = ana_array.stack(grid=('rlat', 'rlon', 'vgrid'))
            ds_analysis = postprocess_cosmo(ana_array, ds_cosmo)
        else:
            ds_analysis = ds_cosmo
        return ds_analysis

    def correct_vars_cos(self, ds_analysis, ds_background):
        ds_analysis = self.constrain_var(ds_analysis, 'QV', lower_bound=0)
        ds_analysis = self.constrain_var(ds_analysis, 'QC', lower_bound=0)
        ds_analysis = self.constrain_var(ds_analysis, 'QI', lower_bound=0)
        ds_analysis = self.constrain_var(ds_analysis, 'QR', lower_bound=0)
        ds_analysis = self.constrain_var(ds_analysis, 'QS', lower_bound=0)
        ds_analysis = self.constrain_var(ds_analysis, 'QV_S', lower_bound=0)
        ds_analysis = self.constrain_var(
            ds_analysis, 'T', lower_bound=173.15, upper_bound=350
        )
        ds_analysis = self.constrain_var(
            ds_analysis, 'T_S', lower_bound=240, upper_bound=350
        )
        return ds_analysis

    @staticmethod
    def press_int(level_height):
        return 1013.25 * np.power(1 - (0.0065 * level_height / 288.15), 5.255)

    def write_cosmo(self, ds_analysis, start_time, end_time, cycle_config):
        run_dir = self.get_run_dir(start_time, cycle_config)
        in_dir = os.path.join(run_dir, 'input')
        out_dir = os.path.join(run_dir, 'output')
        file_path_bg = os.path.join(
            in_dir, 'ens{0:03d}', self.config['COSMO']['bg_files']
        )
        file_path_ana = os.path.join(out_dir, 'ens{0:03d}')
        self.write_ens_data(
            ds_analysis, file_path_bg, file_path_ana, end_time,
            cycle_config['ENSEMBLE']['size'],
            self.config['COSMO']['assim_vars'],
            file_name_str='laf%Y%m%d%H%M%S.nc'
        )

    def get_clm_background(self, start_time, end_time, cycle_config):
        run_dir = self.get_run_dir(start_time, cycle_config)
        in_dir = os.path.join(
            run_dir,
            'input'
        )
        clm_filename = self.get_clm_filename(end_time)
        file_path_bg = os.path.join(in_dir, 'ens{0:03d}', clm_filename)
        ds_clm_bg = self.load_ens_data(
            file_path=file_path_bg, ens_mems=cycle_config['ENSEMBLE']['size']
        ).load()
        return ds_clm_bg

    @staticmethod
    def get_clm_filename(time_obj):
        timediff_clm = time_obj - time_obj.replace(hour=0, minute=0, second=0)
        timediff_clm_secs = timediff_clm.total_seconds()
        clm_file_name = 'clmoas.clm2.r.{0:s}-{1:05d}.nc'.format(
            time_obj.strftime('%Y-%m-%d'), int(timediff_clm_secs)
        )
        return clm_file_name

    @staticmethod
    def apply_weights_surf_clm(ds_bg, weights, assim_vars):
        if assim_vars:
            bg_array = preprocess_clm(ds_bg, assim_vars).load()
            bg_array = bg_array.unstack('grid')
            bg_mean, bg_perts = bg_array.state.split_mean_perts()
            ana_perts = bg_perts.dot(weights, dims=['ensemble'])
            ana_array = bg_mean + ana_perts
            ana_array = ana_array.rename({'ensemble_2': 'ensemble'})
            ana_array = ana_array.stack(grid=('column', 'vgrid'))
            ds_analysis = postprocess_clm(ana_array, ds_bg)
        else:
            ds_analysis = ds_bg
        return ds_analysis

    def correct_vars_clm(self, ds_analysis, ds_background):
        ds_analysis = ds_analysis.copy()
        tot_water_bg = ds_background['WA'] + ds_background['H2OSOI_LIQ'].sum(
            'levtot') + ds_background['H2OCAN'].values
        tot_water_ana = ds_analysis['WA'] + ds_analysis['H2OSOI_LIQ'].sum(
            'levtot') + ds_analysis['H2OCAN'].values
        delta_water = tot_water_ana - tot_water_bg
        ds_analysis['WA'] = ds_analysis['WA']-delta_water
        ds_analysis = self.constrain_var(ds_analysis, 'WA', lower_bound=0)
        ds_analysis = self.constrain_var(
            ds_analysis, 'H2OSOI_LIQ', lower_bound=0
        )
        ds_analysis = self.constrain_var(ds_analysis, 'H2OCAN', lower_bound=0)
        ds_analysis = self.constrain_var(
            ds_analysis, 'T_SOISNO', lower_bound=250, upper_bound=350
        )
        return ds_analysis

    def write_clm(self, ds_analysis, start_time, end_time, cycle_config):
        run_dir = self.get_run_dir(start_time, cycle_config)
        in_dir = os.path.join(run_dir, 'input')
        out_dir = os.path.join(run_dir, 'output')
        file_path_bg = os.path.join(in_dir, 'ens{0:03d}',
                                    self.get_clm_filename(end_time))
        file_path_ana = os.path.join(out_dir, 'ens{0:03d}')
        self.write_ens_data(
            ds_analysis, file_path_bg, file_path_ana, end_time,
            cycle_config['ENSEMBLE']['size'],
            self.config['CLM']['assim_vars'],
            file_name_str='clm_ana%Y%m%d%H%M%S.nc'
        )

    @staticmethod
    def expand_dir(in_dir, cycle_config):
        if not os.path.isabs(in_dir):
            in_dir = os.path.join(
                cycle_config['EXPERIMENT']['path'], in_dir
            )
        return in_dir

    def load_ens_data(self, file_path, ens_mems):
        self.logger.debug('Source file path: {0:s}'.format(file_path))
        ens_mems_list = np.arange(1, ens_mems+1)
        ens_ds_list = [
            xr.open_mfdataset(file_path.format(mem))
            for mem in ens_mems_list
        ]
        ens_ds = xr.concat(ens_ds_list, dim='ensemble')
        ens_ds['ensemble'] = ens_mems_list
        return ens_ds

    def write_ens_data(self, ens_ds, source_path, target_path, end_time,
                       ens_mems, assim_vars, file_name_str='laf%Y%m%d%H%M%S.nc'
                       ):
        self.logger.debug(
            'Example source path: {0:s}'.format(source_path)
        )
        self.logger.debug(
            'Example target path: {0:s}'.format(target_path)
        )
        ens_mems_list = np.arange(1, ens_mems+1)
        for mem in ens_mems_list:
            path_src_mem = list(sorted(glob.glob(source_path.format(mem))))[0]
            trg_dir = target_path.format(mem)
            path_trg_mem = os.path.join(
                trg_dir, end_time.strftime(file_name_str)
            )
            copyfile(path_src_mem, path_trg_mem)
            self.logger.info(
                'Copied file {0:s} to {1:s}'.format(path_src_mem, path_trg_mem)
            )
            tmp_ds = ens_ds.sel(ensemble=mem)
            with nc4.Dataset(path_trg_mem, mode='r+') as mem_ds:
                for var_name in assim_vars:
                    mem_ds[var_name][:] = tmp_ds[var_name]
                    self.logger.info(
                        'Overwritten {0:s} in {1:s} with new data'.format(
                            var_name, path_trg_mem
                        )
                    )

    def gen_weights(self, ds_first_guess, ds_obs):
        ds_first_guess_unstacked = ds_first_guess.unstack('grid').stack(
            grid=['rlat', 'rlon'])
        innov, hx_perts, obs_cov, obs_grid = self.algorithm._prepare(
            ds_first_guess, (ds_obs,)
        )
        back_state = ds_first_guess_unstacked.transpose(
            'grid', 'var_name', 'time', 'vgrid', 'ensemble'
        )
        state_mean, state_perts = back_state.state.split_mean_perts()
        back_prec = self.algorithm._get_back_prec(len(back_state.ensemble))
        innov, hx_perts, obs_cov, back_state = self.algorithm._states_to_torch(
            innov, hx_perts, obs_cov, state_perts.values,
        )
        state_grid = state_perts.grid.values
        len_state_grid = len(state_grid)
        grid_inds = range(len_state_grid)
        delta_ana = []
        weights = []
        self.logger.info('Iterating through state grid')
        for grid_ind in grid_inds:
            tmp_ana_l, w_l, _ = local_etkf(
                self.algorithm._gen_weights_func, grid_ind, innov, hx_perts,
                obs_cov, back_prec, obs_grid, state_grid, back_state,
                self.algorithm.localization,
            )
            delta_ana.append(tmp_ana_l)
            weights.append(w_l)
        delta_ana = torch.stack(delta_ana, dim=0)
        delta_ana = state_perts.copy(data=delta_ana.numpy())
        weights = torch.stack(weights, dim=0).numpy()
        weights_gridded = self.algorithm._get_weight_array(
            weights, delta_ana.indexes['grid'], delta_ana.ensemble.values
        ).unstack('grid')
        return weights_gridded

    @staticmethod
    def ll_to_cartesian(lat_lon, earth_radius=6371000):
        """
        Based on spherical calculations
        """
        lat_rad = np.deg2rad(lat_lon[..., 0])
        lon_rad = np.deg2rad(lat_lon[..., 1])

        x = earth_radius * np.cos(lat_rad) * np.cos(lon_rad)
        y = earth_radius * np.cos(lat_rad) * np.sin(lon_rad)
        z = earth_radius * np.sin(lat_rad)
        return x, y, z

    def interpolate_weights(self, lat_lon_src, lat_lon_trg, weights,
                            stack_coords=('rlat', 'rlon')):
        cart_src = np.stack(
            self.ll_to_cartesian(lat_lon_src), axis=-1
        ).reshape(-1, 3)
        cart_trg = np.stack(
            self.ll_to_cartesian(lat_lon_trg), axis=-1
        ).reshape(-1, 3)
        weights_stacked = weights.stack(grid=stack_coords)

        tree = scipy.spatial.cKDTree(cart_src)
        dist, neighbors = tree.query(cart_trg, k=4)
        inv_dist_squared = 1 / np.power(dist, 2)
        lam = inv_dist_squared / np.sum(inv_dist_squared, axis=-1)[:, None]

        weight_neighbors = weights_stacked.values[..., neighbors]
        weight_interp = np.sum(weight_neighbors * lam, axis=-1)

        non_grid_dims = [dim for dim in weights_stacked.dims if dim != 'grid']
        weight_interp = xr.DataArray(
            weight_interp,
            coords={d: weights_stacked[d] for d in non_grid_dims},
            dims=weights_stacked.dims
        )
        return weight_interp

    @staticmethod
    def describe_arr(array, var_name):
        try:
            dim_height = [d for d in array.dims if d in _height_vars][0]
            vertical_vals = array[dim_height].values
        except IndexError:
            array = array.expand_dims('level')
            vertical_vals = [0, ]
        dims_data = [d for d in array.dims if d not in _height_vars]
        var_min = array.min(dim=dims_data).to_pandas()
        var_max = array.max(dim=dims_data).to_pandas()
        var_mean = array.mean(dim=dims_data).to_pandas()
        var_std = array.std(dim=dims_data).to_pandas()
        var_per = array.quantile([0.1, 0.5, 0.9], dim=dims_data).T.to_pandas()
        var_per.columns = ['10 %', 'median', '90 %']
        var_mai = np.abs(array).mean(dim=dims_data).to_pandas()
        var_df = pd.DataFrame(
            data={'min': var_min, 'max': var_max, 'mean': var_mean,
                  'std': var_std, 'MAI': var_mai},
            index=np.arange(len(vertical_vals)))
        var_df = pd.concat([var_df, var_per], axis=1)
        var_df.index = pd.MultiIndex.from_product(
            [[var_name, ], vertical_vals],
            names=['var_name', 'height']
        )
        return var_df

    def write_info_df(self, array, filename, start_time, cycle_config,
                      assim_vars):
        info_df = pd.DataFrame(
            columns=['min', 'mean', 'max', 'std', '10 %', 'median', '90 %',
                     'MAI']
        )
        for var in assim_vars:
            var_df = self.describe_arr(array[var], var)
            var_df = var_df.reindex(info_df.columns, axis=1)
            info_df = pd.concat([info_df, var_df], axis=0)
        info_text = tabulate(info_df, headers='keys', tablefmt='psql') + '\n'
        run_dir = self.get_run_dir(start_time, cycle_config)
        out_dir = os.path.join(run_dir, 'output')
        file_path_info = os.path.join(out_dir, filename)
        with open(file_path_info, 'a+') as fh_info:
            fh_info.write(info_text)

    def info_assimilation(self, ds_analysis, ds_background, start_time,
                          cycle_config, assim_vars):
        analysis_mean = ds_analysis.mean('ensemble')
        background_mean = ds_background.mean('ensemble')
        impact = analysis_mean-background_mean
        self.write_info_df(impact, 'info_impact.txt', start_time,
                           cycle_config, assim_vars)
        self.write_info_df(background_mean, 'info_background.txt', start_time,
                           cycle_config, assim_vars)
        self.write_info_df(analysis_mean, 'info_analysis.txt', start_time,
                           cycle_config, assim_vars)

    @staticmethod
    def constrain_var(array, var_name, lower_bound=None, upper_bound=None):
        array = array.copy()
        var_array = array[var_name]
        if lower_bound is not None:
            var_array = var_array.where(var_array > lower_bound, lower_bound)
        if upper_bound is not None:
            var_array = var_array.where(var_array < upper_bound, upper_bound)
        array[var_name] = var_array
        return array
