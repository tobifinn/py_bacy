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

# System modules
import logging

# External modules

# Internal modules
from .intf_pytassim import cosmo
from .pytassim import PyTassimModule


logger = logging.getLogger(__name__)


class PytassimCOSMO(PyTassimModule):
    @property
    def module(self):
        return cosmo
