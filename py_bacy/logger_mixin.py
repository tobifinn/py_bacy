#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 05.10.20
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2020}  {Tobias Sebastian Finn}
#

# System modules
import datetime
import logging

# External modules

# Internal modules
import os

logger = logging.getLogger(__name__)


class LoggerMixin(object):
    @property
    def logger(self):
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )

        filename = datetime.datetime.utcnow().strftime('%Y%m%d_%H.log')
        path_log_file = os.path.join(self.config['log_dir'], filename)
        #fh = logging.FileHandler(path_log_file)
        #fh.setFormatter(formatter)

        component = "{}.{}".format(type(self).__module__, type(self).__name__)
        logger = logging.getLogger(component)
        #logger.addHandler(fh)
        return logger