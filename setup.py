#!/bin/env python
# -*- coding: utf-8 -*-
"""
Created on 19.02.17

Created for pymepps-streaming

@author: Tobias Sebastian Finn, tobias.sebastian.finn@studium.uni-hamburg.de

    Copyright (C) {2017}  {Tobias Sebastian Finn}

    This program is free software: you can redistribute it and/or modify
    it under the terms of the GNU General Public License as published by
    the Free Software Foundation, either version 3 of the License, or
    (at your option) any later version.

    This program is distributed in the hope that it will be useful,
    but WITHOUT ANY WARRANTY; without even the implied warranty of
    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
    GNU General Public License for more details.

    You should have received a copy of the GNU General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
"""
# System modules
from setuptools import setup, find_packages
from codecs import open
from os import path

# External modules

# Internal modules

BASE = path.abspath(path.dirname(__file__))

#with open(path.join(BASE, 'README.md'), encoding='utf-8') as f:
#    long_description = f.read()
long_description = ""

setup(
    name='py_bacy',

    version='0.1',

    description='A bacy in python based on bacy of DWD',
    long_description=long_description,

    url='https://gitlab.dkrz.de/u300636/py_bacy',

    author='Tobias Sebastian Finn',
    author_email='t.finn@meteowindow.com',

    license='GPL3',

    packages=find_packages(exclude=['contrib', 'docs', 'tests'])
)