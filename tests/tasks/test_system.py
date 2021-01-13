#!/bin/env python
# -*- coding: utf-8 -*-
#
# Created on 13.01.21
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
from mock import patch, call

# External modules
from prefect import unmapped, Flow

# Internal modules
from py_bacy.tasks.system import create_directory_structure
from py_bacy.tasks.utils import unzip_mapped_result


logging.basicConfig(level=logging.DEBUG)

BASE_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
DATA_PATH = os.path.join(os.path.dirname(BASE_PATH), 'data')


class TestSystemTasks(unittest.TestCase):
    @patch(
        'py_bacy.tasks.system.create_folders.run',
        side_effect=('input/ens001', 'output/ens001',
                     'input/ens002', 'output/ens002')
    )
    def test_create_io_calls_create_folders(self, create_patch):
        _ = create_directory_structure.run(
            ('input', 'output'), 'test', 'ens001'
        )
        create_patch.assert_has_calls((
            call('test/input/ens001'),
            call('test/output/ens001'),
        ))

    @patch(
        'py_bacy.tasks.system.create_folders.run',
        side_effect=(
                'input/ens001', 'output/ens001',
                'input/ens002', 'output/ens002',
                'input/ens003', 'output/ens003'
        )
    )
    def test_create_io_calls_create_folders_mapping(self, create_patch):
        with Flow('test_flow') as test_flow:
            ens_suffixes = ['ens001', 'ens002', 'ens003']
            io_output = create_directory_structure.map(
                unmapped(['input', 'output']), unmapped('test'), ens_suffixes
            )
        state = test_flow.run()
        create_patch.assert_has_calls((
            call('test/input/ens001'),
            call('test/output/ens001'),
            call('test/input/ens002'),
            call('test/output/ens002'),
            call('test/input/ens003'),
            call('test/output/ens003'),
        ))
        print(state.result[io_output].result)

    @patch(
        'py_bacy.tasks.system.create_folders.run',
        side_effect=(
                'input/ens001', 'output/ens001',
                'input/ens002', 'output/ens002',
                'input/ens003', 'output/ens003'
        )
    )
    def test_create_io_calls_create_folders_mapping_unzip(self, create_patch):
        with Flow('test_flow') as test_flow:
            ens_suffixes = ['ens001', 'ens002', 'ens003']
            io_output = create_directory_structure.map(
                unmapped(['input', 'output']), unmapped('test'), ens_suffixes
            )
            unzipped_result = unzip_mapped_result(io_output)
        state = test_flow.run()
        io_result = state.result[unzipped_result].result
        self.assertEqual(len(io_result), 2)
        self.assertTupleEqual(
            io_result[0], ('test/input/ens001', 'test/input/ens002',
                           'test/input/ens003')
        )
        self.assertTupleEqual(
            io_result[1], ('test/output/ens001', 'test/output/ens002',
                           'test/output/ens003')
        )

    @patch(
        'py_bacy.tasks.system.create_folders.run',
        side_effect=(
                'input/ens001', 'output/ens001',
                'input/ens002', 'output/ens002',
                'input/ens003', 'output/ens003'
        )
    )
    def test_create_io_unzip_nout(self, create_patch):
        with Flow('test_flow') as test_flow:
            ens_suffixes = ['ens001', 'ens002', 'ens003']
            io_output = create_directory_structure.map(
                unmapped(['input', 'output']), unmapped('test'), ens_suffixes
            )
            input_dirs, output_dirs = unzip_mapped_result(
                io_output, task_args=dict(nout=2)
            )
        state = test_flow.run()
        input_result = state.result[input_dirs].result
        output_result = state.result[output_dirs].result
        print(input_result)
        self.assertTupleEqual(
            input_result, ('test/input/ens001', 'test/input/ens002',
                           'test/input/ens003')
        )
        self.assertTupleEqual(
            output_result, ('test/output/ens001', 'test/output/ens002',
                            'test/output/ens003')
        )


if __name__ == '__main__':
    unittest.main()
