#!/bin/env python
# -*- coding: utf-8 -*-
#
#Created on 20.03.17
#
#Created for py_bacy
#
#@author: Tobias Sebastian Finn, tobias.sebastian.finn@studium.uni-hamburg.de
#
#    Copyright (C) {2017}  {Tobias Sebastian Finn}
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
import os

# External modules

# Internal modules
from py_bacy.cycle import Cycle
from py_bacy.int2lm import Int2lmModule
from py_bacy.cosmo import CosmoModule
from py_bacy.letkf_single import LetkfSingleModule

def main():
    logging.basicConfig(level=logging.DEBUG)
    configs = '.'
    cycle = Cycle(os.path.join(configs, 'cycle.yml'))
    cycle.register_model(LetkfSingleModule, 'letkf_hh', parent=None,
                         config_path=os.path.join(
                             configs, 'letkf_hh_single.yml'))
    cycle.run()

if __name__ == '__main__':
    main()

