#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 06.01.21
#
# Created for py_bacy
#
# @author: Tobias Sebastian Finn, tobias.sebastian.finn@uni-hamburg.de
#
#    Copyright (C) {2021}  {Tobias Sebastian Finn}


# System modules
import unittest
import logging
import os
from mock import patch, mock_open

# External modules

# Internal modules
from py_bacy.tasks.model import *


BASE_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_PATH = os.path.join(os.path.dirname(BASE_PATH), 'data')


class TestModeltasks(unittest.TestCase):
    def test_modify_template_opens_template_path(self):
        namelist_modifier = ModifyNamelist()
        with patch(
                'builtins.open', mock_open(read_data='open is mocked')
        ) as mocked_open:
            returned_template = namelist_modifier.get_template('test.nml')
        mocked_open.assert_called_once_with('test.nml', mode='r')
        self.assertEqual(returned_template, 'open is mocked')

    def test_modify_template_run_calls_get_template(self):
        with patch('py_bacy.tasks.model.ModifyNamelist.get_template',
                   return_value='get_template called') as template_mock:
            namelist_modifier = ModifyNamelist()
            returned_template = namelist_modifier.run('test.nml', dict())
        template_mock.assert_called_once_with('test.nml')
        self.assertEqual(returned_template, 'get_template called')

    def test_modify_template_replaces_placeholder_with_key_value(self):
        namelist_modifier = ModifyNamelist()
        placeholder_dict = {'%PLACEHOLDER%': 'test', '%TEST%': 'wrong'}
        with patch(
                'builtins.open', mock_open(read_data='%PLACEHOLDER% 123')
        ) as mocked_open:
            replaced_template = namelist_modifier.run(
                'test.nml', placeholder_dict
            )
        mocked_open.assert_called_once_with('test.nml', mode='r')
        self.assertEqual(replaced_template, 'test 123')

    @patch('subprocess.call')
    def test_initialize_namelist_writes_namelist(self, _):
        m = mock_open()
        with patch('builtins.open', m):
            namelist_initializer = InitializeNamelist('')
            namelist_initializer.write_template('test 123', 'test.nml')
        m.assert_called_once_with('test.nml', mode='w')
        handle = m()
        handle.write.assert_called_once_with('test 123')

    @patch('builtins.open', mock_open())
    def test_initialize_namelist_makes_chmod_namelist(self):
        with patch('subprocess.call') as mocked_call:
            namelist_initializer = InitializeNamelist('')
            namelist_initializer.write_template('test 123', 'test.nml')
        mocked_call.assert_called_once_with(['chmod', '755', 'test.nml'])

    @patch('subprocess.Popen', return_value=None)
    @patch('py_bacy.tasks.model.InitializeNamelist.write_template')
    def test_run_inserts_path_kwargs_into_target_path(self, mocked_write, _):
        namelist_initializer = InitializeNamelist('abs_path/test{test:s}.nml')
        _ = namelist_initializer.run('test.nml', 0, test='123')
        mocked_write.assert_called_with(
            namelist='test.nml', target_path='abs_path/test123.nml'
        )

    @patch('subprocess.Popen', return_value=None)
    @patch('py_bacy.tasks.model.InitializeNamelist.write_template')
    def test_run_calls_popen_to_initialize_namelists(self, _, mocked_popen):
        namelist_initializer = InitializeNamelist('abs_path/test{test:s}.nml')
        _ = namelist_initializer.run('test.nml', 12, test='123')
        mocked_popen.assert_called_with(
            ['abs_path/test123.nml', '12']
        )


if __name__ == '__main__':
    unittest.main()
