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
import time
import glob
import shutil

# External modules
import cdo

# Internal modules
from .letkf import LetkfModule
from .model import ModelModule
from .utilities import check_if_folder_exist_create


logger = logging.getLogger(__name__)
CDO = cdo.Cdo()


class LetkfInOutModule(LetkfModule):
    def __init__(self, name, parent, config):
        super().__init__(name, parent, config)

    def run(self, start_time, analysis_time, parent_model, cycle_config):
        run_dir = self.get_run_dir(start_time, cycle_config)
        in_dir = os.path.join(
            run_dir,
            'input'
        )
        out_dir = os.path.join(
             run_dir,
            'output'
        )
        ana_dir = os.path.join(
            run_dir,
            'analysis'
        )
        check_if_folder_exist_create(in_dir)
        check_if_folder_exist_create(out_dir)
        self.symlink(out_dir, ana_dir)
        self.create_symbolic(start_time, analysis_time, parent_model,
                             cycle_config, in_dir)
        self.copy_in_to_output(in_dir, out_dir, analysis_time)
        self.regrid_append_hhl(out_dir, analysis_time, cycle_config)
        self.analysis_simlink(ana_dir, analysis_time, cycle_config)

    def copy_in_to_output(self, in_dir, out_dir, analysis_time):
        in_files = os.path.join(
            in_dir,
            'lff*'
        )
        logger.info(in_files)
        in_files = list(sorted(glob.glob(in_files)))
        for in_file in in_files:
            filename, member = os.path.splitext(in_file)
            ana_filename = 'laf{0:s}{1:s}'.format(
                analysis_time.strftime('%Y%m%d%H%M%S'), member)
            ana_file = os.path.join(out_dir, ana_filename)
            shutil.copyfile(in_file, ana_file)
            logger.info('Copied the input to {0:s}'.format(ana_file))
