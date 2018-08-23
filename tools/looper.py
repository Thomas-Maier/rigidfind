#!/usr/bin/env python

import json
import os
import time
from sys import stdout
from helper import Format

class Looper:
    def __init__(self, file_names, reader, output_name = None, node_path = None):
        self._file_names = file_names
        ## This one defines how the input files are to be read
        self._reader = reader
        if output_name: self._reader.set_output_name(output_name)
        self._bar_mode = True
        try:
            from tqdm import tqdm
        except ImportError:
            self._bar_mode = False
        self._node_path = node_path
        if self._node_path is not None:
            try:
                import tables
            except ImportError:
                print 'Node path defined but no tables module found'
                raise

    def _initialise(self):
        return 0

    def _execute(self, file_name):
        ## Input taken as h5 file if node_path is defined
        if self._node_path is not None:
            self._load_tables(file_name)
        else:
            self._load_dump(file_name)

    def _load_tables(self, file_name):
        import tables
        data_file = tables.open_file(file_name, 'r')
        data = data_file.get_node(self._node_path)
        self._reader.get_data(data, Format.h5)
        data_file.close()

    def _get_data(self, file_obj):
        try:
            import json
            data = json.load(file_obj)
            input_format = Format.json
        except ValueError:
            import pickle
            data = pickle.load(file_obj)
            input_format = Format.pkl

        return data, input_format

    def _load_dump(self, file_name):
        import gzip
        file_obj = gzip.open(file_name, 'rb')
        try:
            data, input_format = self._get_data(file_obj)
        except IOError:
            file_obj = open(file_name, 'rb')
            data, input_format = self._get_data(file_obj)
        self._reader.get_data(data, input_format)
        file_obj.close()

    def _finalise(self):
        self._reader.write_output()

    def just_do_it(self):
        self._initialise()
        if self._bar_mode:
            from tqdm import tqdm
            iter_tuple = enumerate(tqdm(self._file_names, bar_format = '{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}'))
        else:
            iter_tuple = enumerate(self._file_names)
        for i, file_name in iter_tuple:
            if not self._bar_mode:
                ## Extra whitespaces to avoid overflow
                print_string = 'Processing {0} ({1}/{2})                '.format(file_name.rsplit('/', 1)[-1], i+1, len(self._file_names))
                stdout.write('\r'+print_string)
                stdout.flush()
            self._execute(file_name)
        if not self._bar_mode:
            stdout.write('\n')
        self._finalise()
