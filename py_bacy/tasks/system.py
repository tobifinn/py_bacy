#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 05.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import logging
import os
from typing import Iterable, List
import tempfile

# External modules
from prefect import task, context

# Internal modules


logger = logging.getLogger(__name__)


__all__ = [
    'symlink',
    'create_folders',
    'create_directory_structure',

]


@task
def symlink(source: str, target: str) -> str:
    """
    Symlink a given source path to given target path. This is done with an
    atomic operation. If the target path already exists, it will be
    overwritten by the symbolic link.

    Parameters
    ----------
    source : str
        This is the source path, which have to be available.
    target : str
        This is the target path, which will be overwritten if it already exists.

    Returns
    -------
    target: str
        The path to the linked target.

    Raises
    ------
    ValueError
        A ValueError is raised if the source path does not exists.
    """
    if not os.path.exists(source):
        raise ValueError(
            'Give source path {0:s} doesn\'t exists!'.format(
                source
            )
        )
    context.logger.debug('Symlink: {0:s} -> {1:s}'.format(source, target))
    temp_name = next(tempfile._get_candidate_names())
    tmp_file = os.path.join(os.path.dirname(target), temp_name)
    os.symlink(source, tmp_file)
    os.replace(tmp_file, target)
    return target


@task
def create_folders(dir_path: str) -> str:
    """
    Create folder structure with given path.

    Parameters
    ----------
    dir_path : str
        This folder path is created.

    Returns
    -------
    dir_path : str
        The created folder path.

    Raises
    ------
    OSError
        An OSError is raised if after creation the directory path is not
        available.
    """
    if not os.path.isdir(dir_path):
        os.makedirs(dir_path)
    if not os.path.isdir(dir_path):
        raise OSError(
            'Couldn\'t initialize the directory path {0:s}'.format(
                dir_path
            )
        )
    return dir_path


@task
def create_directory_structure(
        directories: Iterable[str],
        run_dir: str,
        ens_suffix: str,
) -> List[str]:
    """
    Construct a directory structure for PyBaCy. The structure is
    iteratively created by: `run_dir/dir/ens_suffix` where dir is the
    iteratively selected directory given by the `directories` keyword.

    Parameters
    ----------
    directories : Iterable[str]
        These directories are created with this function.
    run_dir : str
        This is the basis directory, where the ensemble suffixes and
        directories are created
    ens_suffix : str
        This is the ensemble suffix that is appended to given run dir and
        directory.

    Returns
    -------
    created_directories : List[str]
        These are the paths to the created directories.
    """
    created_directories = []
    for dir_name in directories:
        dir_path = os.path.join(run_dir, dir_name, ens_suffix)
        _ = create_folders(dir_path)
        created_directories.append(dir_path)
    return created_directories
