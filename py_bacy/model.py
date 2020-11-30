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
import abc
import os
import subprocess

# External modules
import yaml

# Internal modules


logger = logging.getLogger(__name__)


class ModelModule(object):
    def __init__(self, name, parent=None, config=None):
        self.name = name
        self.parent = parent
        self.config = self.decode_config(config)
        self.replacement_dict = {
            '%NAME%': self.name,
            '%PROGRAM_DIR%': self.config['program'],
            '%LOG_DIR%': self.config['log_dir'],
        }

    def decode_config(self, config):
        """
        Method to decode a given config. The given config could be a string or a
        dict with given config parameters.

        Parameters
        ----------
        config: str or dict
            The given configuration. This could be a path to a yaml
            configuration file. The configuration could also be a dict with
            given parameter names as key and the settings as value.

        Returns
        -------
        config
        """
        if isinstance(config, str):
            try:
                with open(config, 'r') as f_yaml:
                    decoded_config = yaml.load(f_yaml)
                    logger.info(
                        'The config file for {0:s} was loaded '
                        'successfully!'.format(self.name))
            except FileNotFoundError or IOError:
                raise ValueError(
                    'The given config file {0:s} couldn\'t be found!'.format(
                        config))
        elif isinstance(config, dict):
            decoded_config = config
            logger.info(
                'The config for {0:s} was given by dict!'.format(self.name))
        else:
            raise TypeError(
                'The config for {0:s} should be a string or a dict!'.format(
                    self.name))
        return decoded_config

    @staticmethod
    def symlink(source_path, target_path, *args, **kwargs):
        """
        Method to establish a symbolic link between the source path and the
        target path. If the target path already exists, the source is checked.

        Parameters
        ----------
        source_path: str
            The source path of the symbolic link.
        target_path: str
            The symbolic link will be created at this path.

        Returns
        -------
        None
        """
        try:
            os.symlink(source_path, target_path, *args, **kwargs)
        except FileExistsError:
            logger.debug(
                'The target path {0:s} already exists'.format(target_path))
            os.remove(target_path)
            os.symlink(source_path, target_path, *args, **kwargs)

    @staticmethod
    def check_absolute(path, cycle_config):
        """
        Method to check if a path is an absolute path. If this is not an
        absolute path the experiment path given by the cycle config will be
        prepended.

        Parameters
        ----------
        path: str
            This path will be checked.
        cycle_config: dict
            A dict with the cycle configuration.

        Returns
        -------
        run_dir: str
            The absolute path.
        """
        if not os.path.isabs(path):
            run_dir = os.path.join(
                cycle_config['EXPERIMENT']['path'],
                path
            )
        else:
            run_dir = path
        return run_dir

    def get_run_dir(self, time, cycle_config):
        run_dir = os.path.join(
            cycle_config['EXPERIMENT']['path'],
            time.strftime('%Y%m%d_%H%M'),
            self.name
        )
        return run_dir

    @staticmethod
    def is_running(in_dir):
        pid_path = os.path.join(in_dir, 'pid_file')
        is_running = False
        try:
            with open(pid_path, mode='r') as pid_file:
                pid_strings = pid_file.read()
            pids = [pid for pid in pid_strings.split('\n') if pid != '']
            pids_str = ','.join(pids)
            squeue_output = subprocess.check_output(
                ['squeue', '--jobs={0:s}'.format(pids_str), ],
                text=True
            )
            if any(pid in squeue_output for pid in pids):
                is_running = True
        except FileNotFoundError:
            pass
        return is_running

    def modify_namelist(self):
        """
        Method to modify the name list template given by template path of the
        configuration.

        Returns
        -------
        copied: str
            Path to the modified name list. The path is the original template
            path prepended by 'temp_'.
        """
        orig = self.config['template']
        splitted_orig = os.path.split(orig)
        copied = os.path.join(
            splitted_orig[0],
            'temp_{0:s}'.format(splitted_orig[1])
        )
        logger.debug('Copied template path: {0:s}'.format(copied))
        with open(orig, mode='r') as orig_file:
            template = orig_file.read()
        for placeholder, value in self.replacement_dict.items():
            template = template.replace(placeholder, str(value))
        with open(copied, mode='w') as copied_file:
            copied_file.write(template)
        return copied

    def run(self, start_time, end_time, parent_model, cycle_config):
        pass

    @abc.abstractmethod
    def execute_script(self, script_path, cycle_config, in_dir, ens_size=None):
        pass
