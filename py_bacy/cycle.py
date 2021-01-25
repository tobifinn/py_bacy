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
import datetime
from copy import deepcopy
import os
import tarfile
import shutil
import warnings

# External modules
import yaml
from dask_jobqueue import SLURMCluster
from dask.distributed import Client, LocalCluster

import numpy as np

# Internal modules
from .utilities import round_time
from .utilities import check_if_folder_exist_create


warnings.warn('This old pipeline system is deprecated and will be removed in '
              'the next version of PyBacy. Please use the new prefect-based '
              'DAG system.', DeprecationWarning)
logger = logging.getLogger(__name__)


class Cycle(object):
    def __init__(self, config_path):
        self.config = self.decode_config(config_path)
        self.models = []
        self.model_methods = {
            'CosmoModule': self.forecast_model,
            'Int2lmModule': self.forecast_model,
            'TSMPModule': self.forecast_model,
            'TSMPFiniteModule': self.forecast_model,
            'TSMPRestartModule': self.forecast_model,
            'LetkfModule': self.analysis_model,
            'LetkfInOutModule': self.analysis_model,
            'PytassimModule': self.analysis_model,
            'PytassimCOSMO': self.analysis_model,
            'PytassimCLM': self.analysis_model,
            'SymbolicModule': self.analysis_model,
            'FinitePertModule': self.analysis_model,
            'SEKFModule': self.analysis_model,
        }

    @staticmethod
    def decode_config(config_path=None):
        try:
            if config_path is not None:
                with open(config_path, 'r') as f_yaml:
                        config = yaml.load(f_yaml)
            else:
                config = {}
        except FileNotFoundError as e:
            logger.error('The config file {0:s} couldn\'t be '
                         'found'.format(config_path))
            raise FileNotFoundError(e)
        logger.info('Loaded the config file {0:s}'.format(config_path))
        return config

    def register_model(self, model_class, name, parent=None, config_path=None):
        try:
            config = default_config = self.config[model_class.__name__]
            logger.debug(default_config)
            loaded_config = self.decode_config(config_path)
            logger.debug(loaded_config)
            config.update(loaded_config)
            logger.debug(config)
            model = model_class(name=name, parent=parent, config=config)
        except KeyError:
            logger.warning(
                'No default configuration for {0:s} was found'.format(
                    model_class.__name__))
            model = model_class(name=name, parent=parent, config=config_path)
        self.models.append(model)

    def convert_decode_time(self, time):
        try:
            time = datetime.datetime.strptime(
                time,
                self.config['TIME']['time_format'])
        except TypeError or ValueError:
            time = int(time)
        return time

    def convert_forecast_times(self, config_forecast_times):
        if not isinstance(config_forecast_times, (list, tuple)):
            time = self.convert_decode_time(config_forecast_times)
            forecast_time = [time, ]
        else:
            forecast_time = []
            for raw_time in config_forecast_times:
                time = self.convert_decode_time(raw_time)
                forecast_time.append(time)
        return forecast_time

    def run(self):
        time = datetime.datetime.strptime(
            self.config['TIME']['start_time'],
            self.config['TIME']['time_format'])
        end_time = datetime.datetime.strptime(
            self.config['TIME']['end_time'],
            self.config['TIME']['time_format'])
        analysis_timedelta = datetime.timedelta(
            seconds=self.config['TIME']['analysis_step']
        )
        lead_timedelta = datetime.timedelta(
            seconds=self.config['TIME']['cycle_lead_time']
        )
        self.init_random()
        self.start_dask_client()
        # Iterate through the times
        try:
            while time < end_time:
                analysis_time = time+analysis_timedelta
                run_end_time = time+lead_timedelta
                logger.info(
                    'Starting with time {0:s}, analysis time {1:s} and '
                    'run end time: {2:s}'.format(
                        time.strftime('%Y-%m-%d %H:%Mz'),
                        analysis_time.strftime('%Y-%m-%d %H:%Mz'),
                        run_end_time.strftime('%Y-%m-%d %H:%Mz'),
                    ))
                self.run_single_time(time, analysis_time, run_end_time)
                logger.info(
                    'Finished with time {0:s}, analysis time {1:s} and '
                    'run end time: {2:s}'.format(
                        time.strftime('%Y-%m-%d %H:%Mz'),
                        analysis_time.strftime('%Y-%m-%d %H:%Mz'),
                        run_end_time.strftime('%Y-%m-%d %H:%Mz'),
                    ))
                time = analysis_time
        finally:
            self.shutdown_dask_client()

    def init_random(self):
        if 'Random' in self.config:
            seed = self.config['Random']['seed']
        else:
            seed = 42
        self.config['RandomState'] = np.random.RandomState(seed)

    def start_dask_client(self):
        if 'CLUSTER' in self.config:
            client, cluster = self.init_client()
        else:
            self.config['CLUSTER'] = {}
            client, cluster = None, None
            logger.warning('No cluster settings was found, I will not '
                           'initialize a dask cluster')
        self.config['CLUSTER']['client'] = client
        self.config['CLUSTER']['cluster'] = cluster

    def init_client(self):
        if self.config['CLUSTER']['slurm']:
            cluster = SLURMCluster(
                account=self.config['EXPERIMENT']['account'],
                project=self.config['EXPERIMENT']['account'],
                memory=self.config['EXPERIMENT']['memory_per_node'],
                log_directory=self.config['CLUSTER']['log_dir'],
                name=self.config['CLUSTER']['job_name'],
                queue=self.config['EXPERIMENT']['partition'],
                walltime=self.config['CLUSTER']['wallclock'],
                n_workers=0,
                nanny=True,
                env_extra=[
                    'export OMP_NUM_THREADS=1',
                ],
                scheduler_options={
                    'dashboard_address': self.config['CLUSTER']['dashport'],
                    'host': os.environ['HOSTNAME']
                },
            )
        else:
            cluster = LocalCluster(
                n_workers=self.config['CLUSTER']['n_workers'],
                threads_per_worker=1, local_dir='/tmp',
                dashboard_address=self.config['CLUSTER']['dashport'],
            )
        executor = Client(cluster)
        logger.info(
            'Initialized pool with {0:d} workers'.format(
                self.config['CLUSTER']['n_workers']
            )
        )
        logger.info(cluster)
        logger.info(executor)
        logger.info('Dashboard: {0}'.format(cluster.dashboard_link))
        return executor, cluster

    def shutdown_dask_client(self):
        try:
            self.config['CLUSTER']['client'].close()
            self.config['CLUSTER']['cluster'].close()
        except AttributeError:
            pass

    def check_model_availability(self, model, finished_models):
        if model.parent is None:
            return True
        elif model.parent in finished_models:
            return True
        else:
            return False

    def extract_obs(self, time):
        rounded_time = round_time(time, 3*3600)[0]
        obs_file = 'a_{0:s}.tar.gz'.format(
            rounded_time.strftime('%Y%m%d%H%M'))
        obs_source = os.path.join(
            self.config['OBS']['obs_path'],
            obs_file
        )
        obs_target_path = os.path.join(
            self.config['EXPERIMENT']['path'],
            time.strftime('%Y%m%d_%H%M'),
            'obs')
        check_if_folder_exist_create(obs_target_path)
        try:
            with tarfile.open(obs_source, mode='r:gz') as tar:
                tar.extractall(path=obs_target_path)
        except Exception as e:
            raise ValueError('The tar file {0:s} couldn\'t be extracted to '
                             '{1:s}, due to {2}'.format(obs_source,
                                                        obs_target_path,
                                                        e))
        shutil.copyfile(
            src=os.path.join(self.config['OBS']['obs_path'], 'blklsttmp'),
            dst=os.path.join(obs_target_path, 'blklsttmp'))
        logger.info('The observations within {0:s} were extracted to '
                    '{1:s}.'.format(obs_source, obs_target_path))

    def run_single_time(self, time, analysis_time, run_end_time):
        if self.config['OBS']['use_obs']:
            self.extract_obs(time)
        not_finished_models = deepcopy(self.models)
        finished_models = {}
        while not_finished_models:
            for model in not_finished_models:
                model_path = model.get_run_dir(time, self.config)
                if os.path.isdir(model_path):
                    finished_models[model.name] = model
                    logger.info('The model path {0:s} already exists, assume '
                                'that the model run was successful, skip the '
                                'model!'.format(model_path))
                elif self.check_model_availability(model, finished_models):
                    model_type = model.__class__.__name__
                    try:
                        parent_model = finished_models[model.parent]
                    except KeyError or TypeError:
                        parent_model = None
                    self.model_methods[model_type](
                        model, time, analysis_time, run_end_time, parent_model)
                    finished_models[model.name] = model
            not_finished_models = [model for model in not_finished_models
                                   if model.name not in finished_models]

    def forecast_model(self, model, time, analysis_time, run_end_time,
                       parent_model):
        config_forecast_times = self.config['TIME']['forecast_times']
        forecast_times = self.convert_forecast_times(config_forecast_times)
        lead_times = self.config['TIME']['forecast_lead_time']
        if not isinstance(lead_times, (list, tuple)):
            lead_times = [lead_times, ]*len(forecast_times)
        if time in forecast_times:
            ind = forecast_times.index(time)
            end_time = time + datetime.timedelta(hours=lead_times[ind])
        elif time.hour in forecast_times:
            ind = forecast_times.index(time.hour)
            end_time = time + datetime.timedelta(hours=lead_times[ind])
        else:
            end_time = run_end_time
        logger.info(
            'Starting the forecast model {0:s} for start time {1:s} and end '
            'time {2:s}'.format(
                model.name,
                time.strftime('%Y-%m-%d %H:%Mz'),
                end_time.strftime('%Y-%m-%d %H:%Mz')
            )
        )
        model.run(time, end_time, parent_model, self.config)

    def analysis_model(self, model, time, analysis_time, run_end_time,
                       parent_model):
        logger.info(
            'Starting the analysis model {0:s} for analysis time {1:s}'.format(
                model.name, analysis_time.strftime('%Y-%m-%d %H:%Mz')
            )
        )
        model.run(time, analysis_time, parent_model, self.config)
