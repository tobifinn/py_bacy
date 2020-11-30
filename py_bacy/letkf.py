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
from .model import ModelModule
from .utilities import check_if_folder_exist_create


logger = logging.getLogger(__name__)
CDO = cdo.Cdo()


class LetkfModule(ModelModule):
    def __init__(self, name, parent, config):
        super().__init__(name, parent, config)
        self.replacement_dict['%fl%'] = '!'
        self.replacement_dict['%use_refl%'] = '4'
        self.replacement_dict['%use_radvel%'] = '4'

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
        ana_dir = os.path.join(
            run_dir,
            'analysis'
        )
        check_if_folder_exist_create(in_dir)
        check_if_folder_exist_create(out_dir)
        self.symlink(out_dir, ana_dir)
        self.replacement_dict['%INPUT_DIR%'] = in_dir
        self.replacement_dict['%OUTPUT_DIR%'] = out_dir
        self.replacement_dict['%FG_FILE%'] = 'lff{0:s}.det'.format(
            start_time.strftime( '%Y%m%d%H%M%S'))
        self.replacement_dict['%FG_ENS%'] = 'lff{0:s}'.format(
            start_time.strftime( '%Y%m%d%H%M%S'))
        self.create_symbolic(start_time, analysis_time, parent_model,
                             cycle_config, in_dir)
        templ_file = self.modify_namelist()
        self.execute_script(templ_file, cycle_config, in_dir)
        self.regrid_append_hhl(out_dir, analysis_time, cycle_config)
        self.analysis_simlink(ana_dir, analysis_time, cycle_config)
        os.remove(templ_file)

    #def regrid_append_hhl(self, out_dir, analysis_time, cycle_config):
        #"""
        #This method is necessary, because the LETKF treated the grib2 file like
        #a grib1 file and the HHL file needs to be appended.
        #"""
        #hhl_file = self._create_timed_hhl(out_dir, analysis_time, cycle_config)
        #orig_files = os.path.join(
            #out_dir,
            #'laf{0:s}*'.format(analysis_time.strftime('%Y%m%d%H%M%S'))
        #)
        #orig_files = list(sorted(glob.glob(orig_files)))
        #for orig_file in orig_files:
            #target_file = orig_file+'_new'
            #temp_files = self.regrid_analysis_file(orig_file,
                                                   #target_file)
            #logger.debug(temp_files)
            #CDO.merge(input=' '.join(temp_files+[hhl_file,]),
                      #output=target_file,
                      #options='-f nc')
            #os.remove(orig_file)
            #shutil.move(target_file, orig_file)
            #[os.remove(temp_f) for temp_f in temp_files]
            #logger.info('Remapped the file and appended hhl to {0:s}'.format(
                #orig_file))

    def regrid_append_hhl(self, out_dir, analysis_time, cycle_config):
        """
        This method is necessary, because the LETKF treated the grib2 file like
        a grib1 file and the HHL file needs to be appended.
        """
        hhl_file = self._create_timed_hhl(out_dir, analysis_time, cycle_config)
        orig_files = os.path.join(
            out_dir,
            'laf{0:s}*'.format(analysis_time.strftime('%Y%m%d%H%M%S'))
        )
        orig_files = list(sorted(glob.glob(orig_files)))
        for orig_file in orig_files:
            p = subprocess.Popen(
                'cat {0:s} >> {1:s}'.format(hhl_file, orig_file),
                shell=True)
            p.wait()
            logger.info('Appended hhl to {0:s}'.format(
                orig_file))

    #def _create_timed_hhl(self, out_dir, analysis_time, cycle_config):
        #"""
        #Method to "retime" the source hhl file.

        #Parameters
        #----------
        #out_dir
        #analysis_time
        #cycle_config

        #Returns
        #-------

        #"""
        #output_file = os.path.join(out_dir, 'hhl_time.nc_temp_hhl')
        #CDO.settaxis(analysis_time.strftime('%Y-%m-%d,%H:%M'),
                     #input=self.config['hhl_file'],
                     #output=output_file,
                     #options='-f nc')
        #return output_file

    def _create_timed_hhl(self, out_dir, analysis_time, cycle_config):
        """
        Method to "retime" the source hhl file.

        Parameters
        ----------
        out_dir
        analysis_time
        cycle_config

        Returns
        -------

        """
        output_file = os.path.join(out_dir, 'hhl_timed')
        time_hhl_chain = [
            '{0:s}/bin/grib_set'.format(cycle_config['grib_api']),
            '-s',
            'dataTime={0:s},dataDate={1:s}'.format(
                analysis_time.strftime('%H%M'),
                analysis_time.strftime('%Y%m%d')),
            '{0:s}'.format(self.config['hhl_file']),
            '{0:s}'.format(output_file)
        ]
        subprocess.call(time_hhl_chain)
        return output_file

    def regrid_analysis_file(self, in_file, target):
        """
        This method is necessary, because the LETKF treated the grib2 file like
        a grib1 file.
        """
        var_grid_files = glob.glob(self.config['CDO_GRID']+'_*')
        variables_own_grid = []
        temp_files = []
        for key, grid_file in enumerate(var_grid_files):
            added_suffix = grid_file[len(self.config['CDO_GRID']):]
            suffix_list = added_suffix.split('_')
            valid_variables = [var for var in suffix_list if var != '']
            logger.debug(
                'Create regridded file for variables {0:s}'.format(
                    ','.join(valid_variables)))
            cdo_input = '-selvar,{0:s} {1:s}'.format(
                ','.join(valid_variables), in_file)
            cdo_output = target+'_temp_{0:d}'.format(key)
            CDO.setgrid(grid_file, input=cdo_input,
                        output=cdo_output, options = '-f nc')
            variables_own_grid.extend(valid_variables)
            temp_files.append(cdo_output)
        if variables_own_grid:
            cdo_input = '-delete,name={0:s} {1:s}'.format(
                ','.join(variables_own_grid), in_file)
        else:
            cdo_input = in_file
        logger.debug(
            'Create temp file with input {0:s}'.format(cdo_input))
        cdo_output = target+'_temp_all'
        CDO.setgrid(self.config['CDO_GRID'],
            input=cdo_input, output=cdo_output, options = '-f nc')
        temp_files.append(cdo_output)
        return temp_files

    def analysis_simlink(self, analysis_dir, analysis_time, cycle_config):
        dst_dir = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            analysis_time.strftime('%Y%m%d_%H%M'),
            'analysis')
        check_if_folder_exist_create(dst_dir)
        if cycle_config['ENSEMBLE']['det']:
            src = os.path.join(
                analysis_dir,
                'laf{0:s}.det'.format(analysis_time.strftime( '%Y%m%d%H%M%S')))
            target = os.path.join(
                dst_dir,
                os.path.basename(src)
            )
            self.symlink(src, target)
        for mem in range(1, cycle_config['ENSEMBLE']['size']+1):
            src = os.path.join(
                analysis_dir,
                'laf{0:s}.{1:03d}'.format(
                    analysis_time.strftime( '%Y%m%d%H%M%S'), mem))
            target = os.path.join(
                dst_dir,
                os.path.basename(src)
            )
            self.symlink(src, target)

    def execute_script(self, script_path, cycle_config, in_dir):
        logger.debug('Call {0:s}'.format(script_path))
        subprocess.call(['chmod', '755', script_path])
        p = subprocess.Popen([script_path,])
        logger.debug('Called the template for {0:s}'.format(self.name))
        running = True
        while running:
            time.sleep(5)
            running = self.is_running(in_dir)
            if running:
                logger.info('{0:s}: {1:s} is running!'.format(
                    datetime.datetime.now().strftime('%Y-%m-%d %H:%M %S'),
                    self.name))

    def create_symbolic(self, start_time, analysis_time, parent_model,
                        cycle_config, in_dir):
        time_delta_ana_start = analysis_time-start_time
        time_delta_ana_start = datetime.datetime(1970,1,1)+time_delta_ana_start
        lff_target = 'lfff{0:02d}{1:s}'.format(
            time_delta_ana_start.day-1, time_delta_ana_start.strftime('%H%M%S'))
        parent_analysis_dir = os.path.join(
            parent_model.get_run_dir(start_time, cycle_config), 'analysis')
        if cycle_config['ENSEMBLE']['det']:
            src_lff = os.path.join(
                parent_analysis_dir,
                'det',
                lff_target
            )
            dst_lff = os.path.join(
                in_dir,
                'lff{0:s}.det'.format(start_time.strftime('%Y%m%d%H%M%S'))
            )
            self.symlink(src_lff, dst_lff)
            src_fof = os.path.join(
                # parent_analysis_dir,
                parent_model.get_run_dir(start_time, cycle_config),
                'output',
                'det',
                'fof_{0:s}.nc'.format(start_time.strftime('%Y%m%d%H%M%S'))
            )
            dst_fof = os.path.join(
                in_dir,
                os.path.basename(src_fof)
            )
            self.symlink(src_fof, dst_fof)
        for mem in range(1, cycle_config['ENSEMBLE']['size']+1):
            src_lff = os.path.join(
                parent_analysis_dir,
                'ens{0:03d}'.format(mem),
                lff_target
            )
            dst_lff = os.path.join(
                in_dir,
                'lff{0:s}.{1:03d}'.format(start_time.strftime('%Y%m%d%H%M%S'),
                                          mem)
            )
            self.symlink(src_lff, dst_lff)
            src_fof = os.path.join(
                # parent_analysis_dir,
                parent_model.get_run_dir(start_time, cycle_config),
                'output',
                'ens{0:03d}'.format(mem),
                'fof_{0:s}.nc'.format(start_time.strftime('%Y%m%d%H%M%S'))
            )
            dst_fof = os.path.join(
                in_dir,
                '{0:s}_ens{1:03d}.nc'.format(
                    os.path.splitext(os.path.basename(src_fof))[0], mem)
            )
            self.symlink(src_fof, dst_fof)
