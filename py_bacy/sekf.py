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
from shutil import copyfile

# External modules
import numpy as np
import pandas as pd
import cartopy.crs as ccrs
import xarray as xr
import netCDF4 as nc4

# Internal modules
from .logger_mixin import LoggerMixin
from .intf_pytassim import utils, cosmo, clm, io
from .model import ModelModule
from .utilities import check_if_folder_exist_create


logger = logging.getLogger(__name__)


class SEKFModule(ModelModule, LoggerMixin):
    def __init__(self, name, parent=None, config=None):
        super().__init__(name, parent, config)

    def run(self, start_time, end_time, parent_model, cycle_config):
        self.create_dirs(start_time, end_time, parent_model, cycle_config)
        self.create_symbolic_input(
            start_time, end_time, parent_model, cycle_config
        )
        self.create_symbolic_cos(start_time, end_time, parent_model,
                                 cycle_config)
        self.assimilate_data(start_time, end_time, parent_model,
                             cycle_config)
        self.create_symbolic_analysis(
            start_time, end_time, parent_model, cycle_config
        )

    def _preprocess_data(self, ds_bg, prep_clm, level_depth, level_sat):
        ds_bg = ds_bg[self.config['assim_vars']].isel(levtot=slice(5, None))
        ds_bg['column'] = prep_clm['column']
        ds_bg = ds_bg.unstack('column')
        ds_bg['levtot'] = np.arange(len(ds_bg['levtot']))
        ds_bg['H2OSOI_LIQ'] = ds_bg['H2OSOI_LIQ'] / level_depth / clm.DENSITY
        return ds_bg

    @staticmethod
    def _postprocess_data(ds_ana, prep_clm, level_depth, level_sat):
        analysis_smi = ds_ana['H2OSOI_LIQ'] / level_sat
        analysis_smi = analysis_smi.clip(min=0, max=1)
        ds_ana['H2OSOI_LIQ'] = analysis_smi * level_sat
        ds_ana['H2OSOI_LIQ'] = ds_ana['H2OSOI_LIQ'] * level_depth * clm.DENSITY
        ds_ana = ds_ana.stack(column=['lat', 'lon'])
        return ds_ana

    def _get_h_jacob(self, ds_bg, ds_fg):
        diff_bg = ds_bg.isel(ensemble=slice(1, None))-ds_bg.isel(ensemble=0)
        diff_fg = ds_fg.isel(ensemble=slice(1, None))-ds_fg.isel(ensemble=0)
        h_jacob = []
        for mem, l in enumerate(self.config['SEKF']['levels']):
            tmp_bg = diff_bg.isel(ensemble=mem).isel(levtot=[l, ], drop=False)
            tmp_fg = diff_fg.isel(ensemble=mem)
            tmp_h_jacob = tmp_fg / tmp_bg
            h_jacob.append(tmp_h_jacob)
        h_jacob = xr.concat(h_jacob, 'levtot').drop('ensemble')
        h_jacob_mask = np.abs(h_jacob) < self.config['SEKF']['upper_limit']
        h_jacob = h_jacob.where(h_jacob_mask, 0)
        return h_jacob

    def _get_gain(self, h_jacob, obs_cov):
        b_matrix = self.config['SEKF']['b_scale'] ** 2
        h_jacob_norm = h_jacob.rename({'time': 'time_1'})
        htr = h_jacob / obs_cov
        htrh = (h_jacob_norm * h_jacob).sum(['time', 'time_1']) / obs_cov
        cov_ana = 1 / (1 / b_matrix + htrh)
        gain = cov_ana * htr
        return gain

    def _get_projections(self):
        rotated_pole = ccrs.RotatedPole(
            pole_longitude=self.config['OBS']['rot_pole']['lon'],
            pole_latitude=self.config['OBS']['rot_pole']['lat']
        )
        plate_carree = ccrs.PlateCarree()
        return rotated_pole, plate_carree

    def _interp_fg(self, fg_data, prep_clm):
        rotated_pole, plate_carree = self._get_projections()
        clm_coords_rotated = rotated_pole.transform_points(plate_carree,
                                                           prep_clm.lon.values,
                                                           prep_clm.lat.values)
        clm_rlon = xr.DataArray(
            clm_coords_rotated[:, 0],
            coords={'column': prep_clm.column}, dims=['column']
        )
        clm_rlat = xr.DataArray(
            clm_coords_rotated[:, 1],
            coords={'column': prep_clm.column}, dims=['column']
        )
        fg_interp = fg_data.interp(
            rlon=clm_rlon, rlat=clm_rlat, method='linear')
        fg_interp = fg_interp.drop(['rlon', 'rlat']).unstack('column')
        return fg_interp

    @staticmethod
    def _estimate_increment(gain, innovation):
        increment = (gain * innovation).sum('time')
        return increment

    @staticmethod
    def _create_analysis(ds_bg, increment):
        det_run = ds_bg.isel(ensemble=0)
        increment = increment.reindex_like(det_run, fill_value=0)
        analysis = det_run + increment
        logger.info('Created analysis')
        return analysis

    @staticmethod
    def _write_info_fields(fields_to_write, run_dir):
        out_dir = os.path.join(run_dir, 'output')
        for field_name, field in fields_to_write.items():
            field_path = os.path.join(out_dir, '{0:s}.nc'.format(field_name))
            field.to_netcdf(field_path)

    def _write_analysis(self, analysis, run_dir, file_path_bg, bg_files,
                        analysis_time):
        det_bg_path = file_path_bg.format(1)
        out_dir = os.path.join(run_dir, 'output')
        det_ana_path = os.path.join(
            out_dir, 'ens001', analysis_time.strftime(clm.ANA_FNAME)
        )
        copyfile(det_bg_path, det_ana_path)
        with nc4.Dataset(det_ana_path, mode='r+') as ana_ds:
            for var_name in self.config['assim_vars']:
                ana_ds[var_name][:, 5:] = analysis[var_name].transpose().values
                logger.info(
                    'Overwritten {0:s} in {1:s} with new data'.format(
                        var_name, det_ana_path
                    )
                )

    def assimilate_data(self, start_time, analysis_time, parent_model,
                        cycle_config):
        cycle_config['CLUSTER']['cluster'].scale(
            cycle_config['CLUSTER']['n_workers']
        )
        run_dir = self.get_run_dir(start_time, cycle_config)
        util_dir = self.config['OBS']['utils_path']
        file_path_obs = self.config['OBS']['path']
        fg_files = self.config['OBS']['fg_files']
        bg_files = self.config['bg_files']
        ens_mems = len(self.config['SEKF']['levels']) + 1
        obs_timedeltas = [
            pd.to_timedelta(td) for td in self.config['OBS']['timedelta']
        ]
        obs_times = [analysis_time + td for td in obs_timedeltas]

        file_path_bg = clm.get_bg_path(run_dir, bg_files, analysis_time)
        ds_bg = io.load_ens_data(
            file_path=file_path_bg, ensemble_members=ens_mems,
            client=cycle_config['CLUSTER']['client']
        )
        prep_clm = clm.load_auxiliary_data(util_dir)
        logger.info('Loaded background')

        ds_const = prep_clm.unstack('column')
        level_depth = ds_const['DZSOI'].drop('levsoi').rename(
            {'levsoi': 'levtot'})
        level_sat = ds_const['WATSAT'].drop('levsoi').rename(
            {'levsoi': 'levtot'})
        ds_bg = self._preprocess_data(ds_bg, prep_clm, level_depth, level_sat)
        logger.info('Preprocessed background')

        ds_obs = io.load_observations(file_path_obs)
        ds_obs = ds_obs.unstack('obs_grid_1').squeeze(drop=True)
        obs_vals = ds_obs['observations'].sel(
            time=obs_times, drop=False
        )
        logger.info('Loaded observations')

        ds_fg = cosmo.load_first_guess(
            run_dir, fg_files, ens_mems, start_time,
            client=cycle_config['CLUSTER']['client']
        )
        fg_t2m = ds_fg['T_2M'].squeeze().sel(time=obs_vals.time, drop=False)
        logger.info('Loaded first guess')

        fg_interp = self._interp_fg(fg_t2m, prep_clm)
        fg_interp = fg_interp.sel(time=obs_vals.time, drop=False)
        innovation = obs_vals - fg_interp.isel(ensemble=0)
        logger.info('Created innovations')

        h_jacob = self._get_h_jacob(ds_bg, fg_interp)
        logger.info('Got jacobians')

        gain = self._get_gain(h_jacob, ds_obs['covariance'])
        logger.info('Got gain')

        increment = self._estimate_increment(gain, innovation)
        logger.info('Estimated increment')

        info_fields = {
            'innovation': innovation,
            'jacobian': h_jacob,
            'gain': gain,
            'increment': increment
        }
        self._write_info_fields(info_fields, run_dir)
        logger.info('Wrote information fields')

        analysis = self._create_analysis(ds_bg, increment)
        logger.info('Created analysis')

        analysis = self._postprocess_data(analysis, prep_clm, level_depth,
                                          level_sat)
        logger.info('Postprocessed analysis')

        self._write_analysis(analysis, run_dir, file_path_bg, bg_files,
                             analysis_time)
        logger.info('Wrote analysis')

        cycle_config['CLUSTER']['cluster'].scale(0)

    def create_dirs(self, start_time, analysis_time, parent_model,
                    cycle_config):
        self.logger.info('Create dirs')
        run_dir_current = self.get_run_dir(start_time, cycle_config)
        dir_input = os.path.join(run_dir_current, 'input')
        dir_output = os.path.join(run_dir_current, 'output')
        dir_analysis = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            analysis_time.strftime('%Y%m%d_%H%M'),
            'analysis'
        )

        ens_mems = len(self.config['SEKF']['levels']) + 1
        for mem in range(1, ens_mems+1):
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            check_if_folder_exist_create(dir_input_mem)
        dir_output = os.path.join(dir_output, 'ens001')
        check_if_folder_exist_create(dir_output)
        analysis_dir = os.path.join(dir_analysis, 'ens001')
        check_if_folder_exist_create(analysis_dir)
        self.logger.info('Finished directory creation')

    def create_symbolic_input(
            self, start_time, analysis_time, parent_model, cycle_config
    ):
        self.logger.info('Starting linking for the input')
        ens_mems = len(self.config['SEKF']['levels']) + 1
        run_dir_current = self.get_run_dir(start_time, cycle_config)

        parent_analysis_dir = os.path.join(
            parent_model.get_run_dir(start_time, cycle_config), 'output'
        )
        finite_pert_out_dir = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            start_time.strftime('%Y%m%d_%H%M'), 'finite_pert', 'output'
        )
        dir_input = os.path.join(run_dir_current, 'input')

        # First guess linking
        fg_file_path = os.path.join(parent_analysis_dir, 'ens{0:03d}',
                                    self.config['OBS']['fg_files'])
        for mem in range(1, ens_mems+1):
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            fg_files = glob.glob(fg_file_path.format(mem))
            for fpath in fg_files:
                fg_fname = os.path.basename(fpath)
                fg_in_path = os.path.join(dir_input_mem, fg_fname)
                self.symlink(fpath, fg_in_path)
        self.logger.info('Finished first guess linking')

        # Background linking
        bg_fname = clm.get_bg_filename(self.config['bg_files'], analysis_time)
        fname_analysis = analysis_time.strftime(clm.ANA_FNAME)
        bg_file_path = os.path.join(
            finite_pert_out_dir, 'ens{0:03d}', fname_analysis
        )

        ens_mems = len(self.config['SEKF']['levels']) + 1
        for mem in range(1, ens_mems+1):
            dir_input_mem = os.path.join(dir_input, 'ens{0:03d}'.format(mem))
            bg_path_mem = bg_file_path.format(mem)
            logger.debug(bg_path_mem)
            bg_path_found = list(sorted(glob.glob(bg_path_mem)))
            bg_path_par = bg_path_found[0]
            bg_path_mem = os.path.join(dir_input_mem, bg_fname)
            self.symlink(bg_path_par, bg_path_mem)
        self.logger.info('Finished background linking')

    def create_symbolic_cos(self, start_time, analysis_time, parent_model,
                            cycle_config):
        logger.info('Linking COSMO analysis')
        run_dir_current = self.get_run_dir(start_time, cycle_config)
        tsmp_analysis_dir = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            start_time.strftime('%Y%m%d_%H%M'), 'terrsysmp',
            'output'
        )
        bg_fname = cosmo.get_bg_filename(self.config['COSMO']['bg_files'],
                                         analysis_time)
        cos_bg_file_path = os.path.join(
            tsmp_analysis_dir, 'ens001', bg_fname
        )
        dir_output = os.path.join(run_dir_current, 'output', 'ens001')
        cos_fname_analysis = analysis_time.strftime(cosmo.ANA_FNAME)
        cos_out_ana_path = os.path.join(dir_output, cos_fname_analysis)

        dir_analysis = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            analysis_time.strftime('%Y%m%d_%H%M'),
            'analysis', 'ens001'
        )
        cos_ana_path = os.path.join(dir_analysis, cos_fname_analysis)

        if self.config['COSMO']['analysis']:
            # Link to output dir
            self.symlink(cos_bg_file_path, cos_out_ana_path)
            self.symlink(cos_bg_file_path, cos_ana_path)
        logger.info('Finished linking COSMO analysis')

    def create_symbolic_analysis(self, start_time, analysis_time, parent_model,
                                 cycle_config):
        logger.info('Linking CLM output to analysis')
        run_dir_current = self.get_run_dir(start_time, cycle_config)
        dir_analysis = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            analysis_time.strftime('%Y%m%d_%H%M'),
            'analysis', 'ens001'
        )

        dir_output = os.path.join(run_dir_current, 'output', 'ens001')

        # CLM linking
        clm_out_path = os.path.join(
            dir_output, analysis_time.strftime(clm.ANA_FNAME)
        )
        clm_ana_path = os.path.join(
            dir_analysis, analysis_time.strftime(clm.ANA_FNAME)
        )
        self.symlink(clm_out_path, clm_ana_path)
        logger.info('Linked CLM output to analysis')
