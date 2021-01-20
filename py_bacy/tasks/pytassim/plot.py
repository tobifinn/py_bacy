#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 13.11.20
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2020}  {Tobias Sebastian Finn}
#

# System modules
import logging
import os

# External modules
import matplotlib as mpl
mpl.use('agg')

import numpy as np
import pandas as pd

import matplotlib.pyplot as plt

# Internal modules



mpl_logger = logging.getLogger('matplotlib')
mpl_logger.setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


def plot_rank_hist(fg_values, obs_values):
    stacked_fg = fg_values.stack(grid_time=['time', 'obs_grid_1'])
    ens_vals = stacked_fg.transpose('grid_time', 'ensemble').values
    ens_vals = np.concatenate([
        np.array([-np.inf]).repeat(ens_vals.shape[0])[:, None],
        np.sort(ens_vals, axis=-1)
    ], axis=1)
    obs_vals = obs_values.values.reshape(-1)
    rank = (obs_vals[:, None] > ens_vals).argmin(axis=-1) - 1
    rank[rank < 0] = 40
    fig, ax = plt.subplots()
    ax.hist(rank, bins=np.arange(-0.5, ens_vals.shape[1]+0.5, 1))
    ax.set_ylabel('Number of occurence')
    ax.set_xlabel('Rank')
    return fig, ax


def plot_histogram(values, x_axis='Differences obs-mean', bins=50):
    fig, ax = plt.subplots()
    hist = ax.hist(values, bins=bins, log=True)
    ax.axvline(ymax=np.array(hist[0]).max() + 10, color='black')
    ax.set_ylabel('Log-Number of occurence')
    ax.set_xlabel(x_axis)
    if isinstance(hist[0], list):
        ax.legend(
            hist[-1], pd.to_datetime(values.time.values).strftime('%H:%M:%S')
        )
    return fig, ax


def write_obs_plots(fg_grouped, obs_grouped, run_dir):
    fg_mean = fg_grouped.mean('ensemble')
    diff_mean = obs_grouped['observations'] - fg_mean
    out_dir = os.path.join(run_dir, 'output')
    for group in diff_mean['obs_group']:
        fig, ax = plot_histogram(
            diff_mean.sel(obs_group=group).values.flatten()
        )
        file_name = 'hist_mean_diff_{0}.png'.format(int(group))
        fig.savefig(os.path.join(out_dir, file_name))
        fig, ax = plot_histogram(
            diff_mean.sel(obs_group=group)
        )
        file_name = 'hist_mean_time_diff_{0}.png'.format(
            int(group)
        )
        fig.savefig(os.path.join(out_dir, file_name))
    diff_ens = obs_grouped['observations'] - fg_grouped
    for group in obs_grouped['observations']['obs_group']:
        group = group.drop('ensemble')
        fig, ax = plot_rank_hist(
            fg_grouped.sel(obs_group=group),
            obs_grouped['observations'].sel(obs_group=group)
        )
        file_name = 'rank_hist_{0}.png'.format(int(group))
        fig.savefig(os.path.join(out_dir, file_name))
        fig, ax = plot_histogram(
            diff_ens.sel(obs_group=group).values.flatten()
        )
        file_name = 'hist_ens_diff_{0}.png'.format(
            int(group)
        )
        fig.savefig(os.path.join(out_dir, file_name))
    plt.cla()