#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 14.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
from typing import Dict, Any, Tuple, Union
import os

# External modules
import prefect
from prefect import task
from prefect.triggers import all_finished

from distributed import Client, LocalCluster
from distributed.deploy import Cluster
from dask_jobqueue import SLURMCluster

# Internal modules


@task
def get_cluster_mode(
        cycle_config: Dict[str, Any],
) -> Union[None, str]:
    """
    Decode the cluster mode from the cycle configuration. Currently only a
    `slurm`, `local`, or None mode are available.

    Parameters
    ----------
    cycle_config : Dict[str, Any]
        The cluster mode is searched within this cycle configuration.

    Returns
    -------
    mode : str or None
        The specified cluster mode. If the mode is None, then no mode was found.
    """
    try:
        if cycle_config['CLUSTER']['slurm']:
            mode = 'slurm'
        else:
            mode = 'local'
    except KeyError:
        mode = None
    return mode


@task
def initialize_slurm_cluster(
        cycle_config: Dict[str, Any],
) -> Tuple[Client, SLURMCluster]:
    """
    Initialize a slurm cluster based on given cycle configuration. A nanny
    will be activatived, whereas `export OMP_NUM_THREADS=1` is set as
    environment option, because this setting speeds up the data assimilation. An
    additional host will be set to the scheduler options to be compatible to
    the super computer `Mistral` at the DKRZ in Hamburg.

    Parameters
    ----------
    cycle_config : Dict[str, Any]
        Information about the slurm cluster are searched within this cycle
        configuration.

    Returns
    -------
    client : distributed.Client
        The initialized client based on the initialized cluster
    cluster : dask_jobqueue.SLURMCluster
        The initialized slurm cluster.
    """
    cluster = SLURMCluster(
        account=cycle_config['EXPERIMENT']['account'],
        project=cycle_config['EXPERIMENT']['account'],
        cores=cycle_config['EXPERIMENT']['cpus_per_node'],
        memory=cycle_config['EXPERIMENT']['memory_per_node'],
        log_directory=cycle_config['CLUSTER']['log_dir'],
        name=cycle_config['CLUSTER']['job_name'],
        queue=cycle_config['EXPERIMENT']['partition'],
        walltime=cycle_config['CLUSTER']['wallclock'],
        n_workers=cycle_config['CLUSTER']['n_workers'],
        nanny=True,
        env_extra=[
            'export OMP_NUM_THREADS=1',
        ],
        scheduler_options={
            'dashboard_address': cycle_config['CLUSTER']['dashport'],
            'host': os.environ['HOSTNAME']
        },
    )
    client = Client(cluster)
    logger = prefect.context.get('logger')
    logger.info(
        'Initialized slum cluster: {0:s} with {1:d} workers'.format(
            str(cluster), cycle_config['CLUSTER']['n_workers'])
    )
    logger.debug(str(client))
    logger.info(
        'Initialized dashboard under: {0}'.format(cluster.dashboard_link)
    )
    return client, cluster


@task
def initialize_local_cluster(
        cycle_config: Dict[str, Any]
) -> Tuple[Client, LocalCluster]:
    """
    Initialize a local cluster based on given cycle configuration. The number of
    threads will be set to 1 and the local directory will point to `/tmp`.

    Parameters
    ----------
    cycle_config : Dict[str, Any]
        Information about the local cluster are searched within the 'CLUSTER'
        keyword. The necessary information is the number of workers, 
        specified with 'n_workers', and the dashboard address specified under
        'dashport'.

    Returns
    -------
    client : distributed.Client
        The initialized client based on the initialized cluster
    cluster : distributed.LocalCluster
        The initialized local cluster.
    """
    cluster = LocalCluster(
        n_workers=cycle_config['CLUSTER']['n_workers'],
        threads_per_worker=1, log_directory='/tmp',
        dashboard_address=cycle_config['CLUSTER']['dashport'],
    )
    client = Client(cluster)
    logger = prefect.context.get('logger')
    logger.info(
        'Initialized local cluster: {0:s} with {1:d} workers'.format(
            str(cluster), cycle_config['CLUSTER']['n_workers'])
    )
    logger.info(
        'Initialized dashboard under: {0}'.format(cluster.dashboard_link)
    )
    return client, cluster


@task
def initialize_none_cluster() -> Tuple[Client, LocalCluster]:
    """
    Initialize a local cluster with only a single worker, because no cluster
    mode was specified. The threads per worker will be set to 1 and the local
    directory will point to `/tmp`.

    Returns
    -------
    client : distributed.Client
        The initialized client with the initialized cluster as argument.
    cluster : distributed.LocalCluster
        The initialized cluster with a single worker.
    """
    cluster = LocalCluster(
        n_workers=1, threads_per_worker=1, log_directory='/tmp',
    )
    client = Client(cluster)
    logger = prefect.context.get('logger')
    logger.info(
        'Initialized None cluster: {0:s} with a single worker'.format(
            str(cluster)
        )
    )
    logger.info(
        'Initialized dashboard under: {0}'.format(cluster.dashboard_link)
    )
    return client, cluster


@task(trigger=all_finished)
def shutdown_cluster(client: Client, cluster: Cluster):
    """
    This task shuts down given client and cluster.

    Parameters
    ----------
    client : distributed.Client
        This client will be closed.
    cluster : distributed.Cluster
        This cluster will be closed after the client is closed.
    """
    client.close()
    cluster.close()


@task
def scale_client(client: Client, n_workers: int = 0) -> int:
    """
    Scale the number of workers within given client

    Parameters
    ----------
    client : distributed.Client
        This client will be scaled.
    n_workers : int, optional
        This specifies the number of workers that should be used. Default is 0.

    Returns
    -------
    old_n_workers : int
        The old number of workers, before the client was scaled.
    """
    old_n_workers = len(client.cluster.workers)
    client.cluster.scale(n_workers)
    logger = prefect.context.get('logger')
    logger.info('Scaled the number of workers from {0:d} to {1:d}'.format(
        old_n_workers, n_workers
    ))
    return old_n_workers
