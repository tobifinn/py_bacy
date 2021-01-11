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
            curr_config = {'template': 'test.nml'}
            returned_template = namelist_modifier.run(curr_config, dict())
        template_mock.assert_called_once_with('test.nml')
        self.assertEqual(returned_template, 'get_template called')

    def test_modify_template_replaces_placeholder_with_key_value(self):
        namelist_modifier = ModifyNamelist()
        placeholder_dict = {'%PLACEHOLDER%': 'test', '%TEST%': 'wrong'}
        with patch(
                'builtins.open', mock_open(read_data='%PLACEHOLDER% 123')
        ) as mocked_open:
            curr_config = {'template': 'test.nml'}
            replaced_template = namelist_modifier.run(
                curr_config, placeholder_dict
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
    def test_run_calls_popen_to_initialize_namelists(self, _, mocked_popen):
        namelist_initializer = InitializeNamelist('test.nml')
        created_folders = {'input': 'abs_path'}
        _ = namelist_initializer.run('test.nml', created_folders, 12)
        mocked_popen.assert_called_with(
            ['abs_path/test.nml', '12']
        )

    def test_construct_ensemble_det_true_det_true(self):
        ensemble_constructor = ConstructEnsemble()
        cycle_config = {'ENSEMBLE': {'det': True}}
        self.assertTrue(ensemble_constructor._deterministic_run(cycle_config))

    def test_construct_ensemble_det_false_det_false(self):
        ensemble_constructor = ConstructEnsemble()
        cycle_config = {'ENSEMBLE': {'det': False}}
        self.assertFalse(ensemble_constructor._deterministic_run(cycle_config))

    def test_construct_ensemble_det_false_key_error(self):
        ensemble_constructor = ConstructEnsemble()
        cycle_config = {'ENSEMBLE': {'size': 40}}
        self.assertFalse(ensemble_constructor._deterministic_run(cycle_config))
        cycle_config = {'ENSEMBLE': None}
        self.assertFalse(ensemble_constructor._deterministic_run(cycle_config))
        cycle_config = dict()
        self.assertFalse(ensemble_constructor._deterministic_run(cycle_config))

    def test_construct_ensemble_returns_suffixes_and_range(self):
        ensemble_constructor = ConstructEnsemble()
        cycle_config = {'ENSEMBLE': {'size': 40}}
        suffixes = ['ens{0:03d}'.format(e) for e in range(1, 41)]
        ens_range = list(range(1, 41))
        ret_suffixes, ret_range = ensemble_constructor.run(cycle_config)
        self.assertListEqual(ret_suffixes, suffixes)
        self.assertListEqual(ret_range, ens_range)

    def test_construct_ensemble_adds_deterministic(self):
        ensemble_constructor = ConstructEnsemble()
        cycle_config = {'ENSEMBLE': {'size': 40, 'det': True}}
        suffixes = ['det'] + ['ens{0:03d}'.format(e) for e in range(1, 41)]
        ens_range = [0] + list(range(1, 41))
        ret_suffixes, ret_range = ensemble_constructor.run(cycle_config)
        self.assertListEqual(ret_suffixes, suffixes)
        self.assertListEqual(ret_range, ens_range)

    def test_check_output_checks_necessary_files(self):
        output_checker = CheckOutput(['bla*.nc'])
        created_folders = {'output': 'test_folder'}
        with patch('glob.glob', return_value=('test.nc', 'test1.nc')) as \
                glob_patch:
            outputted_files = output_checker.run(created_folders)
        self.assertListEqual(['test.nc', 'test1.nc'], outputted_files)
        glob_patch.assert_called_once_with('test_folder/bla*.nc')

    @patch('glob.glob', return_value=[])
    def test_check_output_raises_os_error_if_empty(self, _):
        output_checker = CheckOutput(['bla*.nc'])
        created_folders = {'output': 'test_folder'}
        with self.assertRaises(OSError):
            outputted_files = output_checker.run(created_folders)

    def test_check_output_sorts_files(self):
        output_checker = CheckOutput(['bla*.nc', 'blu*.nc'])
        created_folders = {'output': 'test_folder'}
        with patch('glob.glob', side_effect=(['test.nc'], ['atest.nc'])) as \
                glob_patch:
            outputted_files = output_checker.run(created_folders)
        self.assertListEqual(['atest.nc', 'test.nc'], outputted_files)


if __name__ == '__main__':
    unittest.main()
