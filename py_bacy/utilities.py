#!/bin/env python
# -*- coding: utf-8 -*-
#
#Created on 21.03.17
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
import os

# External modules

# Internal modules


logger = logging.getLogger(__name__)


def round_time(dt=None, roundTo=60):
   """
   Round a datetime object to any time laps in seconds
   dt : datetime.datetime object, default now.
   roundTo : Closest number of seconds to round to, default 1 minute.
   Author: Thierry Husson 2012 - Use it as you want but don't blame me.
   """
   if dt == None : dt = datetime.datetime.now()
   seconds = (dt.replace(tzinfo=None) - dt.min).seconds
   rounding = seconds % roundTo
   timedelta =  datetime.timedelta(0,rounding,-dt.microsecond)
   return dt-timedelta, timedelta

def check_if_folder_exist_create(path):
   """
   This method checks if the dir path exists, if not it will be created.

   Parameters
   ----------
   path: str
       The path to the directory, which should be created.
   """
   if not os.path.isdir(path):
      os.makedirs(path)