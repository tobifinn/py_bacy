#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 5/21/19
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2019}  {Tobias Sebastian Finn}
#
#    This program is free software: you can redistribute it and/or modify
#    it under the terms of the GNU General Public License as published by
#    the Free Software Foundation, either version 3 of the License, or
#    (at your option) any later version.
#
#    This program is distributed in the hope that it will be useful,
#    but WITHOUT ANY WARRANTY; without even the implied warranty of
#    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    GNU General Public License for more details.
#
#    You should have received a copy of the GNU General Public License
#    along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

# System modules
import logging
import argparse

# External modules
import numpy as np
from dask.distributed import Client, LocalCluster
import pandas as pd

# Internal modules
from pytassim.assimilation import DistributedLETKFUncorr
from pytassim.localization import GaspariCohn
from pytassim.model.terrsysmp.clm import preprocess_clm, postprocess_clm

from py_bacy.intf_pytassim import obs_op, clm, io, utils

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


parser = argparse.ArgumentParser(
    description='This script is used to assimilate T2m into CLM files and '
                'to generated analysis files for CLM',
    prog='PyTassim assimilation for CLM'
)
parser.add_argument(
    '--run_dir', type=str, help='Run directory, where all data is stored',
    required=True
)
parser.add_argument(
    '--util_dir', type=str, help='Directory, where utility data is stored',
    required=True
)
parser.add_argument(
    '--path_obs', type=str,
    help='Full path to the NetCDF-file with the observations', required=True
)
parser.add_argument(
    '--start_time', type=utils.parse_datetime,
    help='Start date of this run (format: YYYYmmdd_HHMM)',
    required=True
)
parser.add_argument(
    '--end_time', type=utils.parse_datetime,
    help='End date of this run (format: YYYYmmdd_HHMM)',
    required=True
)
parser.add_argument(
    '--ensemble_members', type=int, default=40,
    help='Number of ensemble members (default=40)'
)
parser.add_argument(
    '--fg_files', type=str, default='*_fg',
    help='Regex suffix for first guess data files (default=`*_fg`)'
)
parser.add_argument(
    '--assim_vars', type=str, default=[], nargs='+',
    help='Variables which should be assimilated (default=No variable)'
)
parser.add_argument(
    '--loc_radius', type=float, default=(50000, 0.7), nargs='+',
    help='Localization radii, which are used to initialize Gaspari-Cohn '
         'localization (horizontal in meters, vertical in meters depth), '
         'default=(50000, 0.7)'
)
parser.add_argument(
    '--inf_factor', type=float, default=1.1,
    help='Inflation factor for LETKF (default=1.1)'
)
parser.add_argument(
    '--n_workers', type=int, default=48,
    help='Number of workers to parallelize LETKF (default=48)'
)
parser.add_argument(
    '--chunksize', type=int, default=1000,
    help='Every worker processes this number of grid points as a batch '
         '(default=1000)'
)


def get_executor(n_workers=48):
    cluster = LocalCluster(n_workers=n_workers, threads_per_worker=1,
                           processes=True)
    executor = Client(cluster)
    logger.info('Initialized pool with {0:d} workers'.format(n_workers))
    logger.info(cluster)
    logger.info(executor)
    logger.info('Dashboard: {0}'.format(cluster.dashboard_link))
    return executor


def init_assim(loc_radius, executor, inf_factor=1.1, chunksize=1000):
    localization = GaspariCohn(
        np.array(loc_radius), dist_func=clm.distance_func
    )
    letkf = DistributedLETKFUncorr(
        executor, chunksize=chunksize, localization=localization,
        inf_factor=inf_factor
    )
    return letkf


def assimilate_clm(
        run_dir, util_dir, file_path_obs, start_time, end_time,
        ensemble_members, fg_files, assim_vars, assim_obj
):
    ds_bg = clm.load_background(run_dir, end_time, ensemble_members)
    ds_const = clm.load_constant_data(util_dir)
    state_bg = clm.get_bg_array(ds_bg, ds_const, end_time, assim_vars,)
    state_fg, observations = obs_op.load_obs_fg_t2m(
        run_dir, fg_files, ensemble_members, start_time, file_path_obs, util_dir
    )
    state_analysis = assim_obj.assimilate(state_bg, observations, state_fg)
    ds_ana = postprocess_clm(state_analysis, ds_bg)
    ds_ana = clm.correct_vars(ds_ana, ds_bg)
    utils.info_assimilation(ds_ana, ds_bg, assim_vars, run_dir, 'clm')
    clm.write_analysis(ds_ana, run_dir, end_time, ensemble_members, assim_vars)


if __name__ == '__main__':
    args = parser.parse_args()
    executor = get_executor(args.n_workers)
    assim_obj = init_assim(args.loc_radius, executor, args.inf_factor,
                           args.chunksize)
    assimilate_clm(
        args.run_dir, args.util_dir, args.path_obs, args.start_time,
        args.end_time, args.ensemble_members, args.fg_files, args.assim_vars,
        assim_obj
    )
