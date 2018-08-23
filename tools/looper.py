#!/usr/bin/env python

import json
import os
import time
from sys import stdout
from helper import Format

class Looper:
    def __init__(self, FileNames, Reader, OutputName = None, InputFormat = Format.h5, NodePath = None):
        self._file_names = FileNames
        self._input_format = InputFormat
        ## This one defines how the input files are to be read
        self._reader = Reader
        if OutputName: self._reader.set_output_name(OutputName)
        self._node_path = NodePath
        if self._input_format == Format.h5 and self._node_path is None:
            print 'Input format is h5, but no node path is provided'
            raise Exception

    def _initialise(self):
        return 0

    def _execute(self, file_name):
        if self._input_format == Format.h5:
            import tables
            data_file = tables.open_file(file_name, 'r')
            data = data_file.get_node(self._node_path)
            self._reader.get_data(data, self._input_format)
            data_file.close()
        else:
            open_func = open
            if self._input_format == Format.gzipjson or self._input_format == Format.gzippkl:
                import gzip
                open_func = gzip.open
            mode = 'r'
            loader = json
            if self._input_format == Format.pkl or self._input_format == Format.gzippkl:
                import pickle
                mode = 'rb'
                loader = pickle
            with open_func(file_name, mode) as in_file:
                data = loader.load(in_file)
            self._reader.get_data(data, self._input_format)

    def _finalise(self):
        self._reader.write_output()

    def just_do_it(self):
        self._initialise()
        n_files = len(self._file_names)
        for i, file_name in enumerate(self._file_names):
            ## Extra whitespaces to avoid overflow
            print_string = 'Processing {0} ({1}/{2})                '.format(file_name.rsplit('/', 1)[-1], i+1, n_files)
            stdout.write('\r'+print_string)
            stdout.flush()
            self._execute(file_name)
        stdout.write('\n')
        self._finalise()
