
from __future__ import print_function
from enum import Enum


class Format(Enum):
    h5 = 0
    json = 1
    gzipjson = 2
    pkl = 3
    gzippkl = 4

def create_table(h5file, node_path, description):
    if node_path.startswith('/') or node_path.endswith('/'): node_path = node_path.strip('/')
    node_path = node_path.split('/')
    group = h5file.root
    for group_name in node_path[:len(node_path)-1]:
        group = h5file.create_group(group, group_name)

    return h5file.create_table(group, node_path[-1], description)

from .reader import Reader, Mode
from .looper import Looper

def get_looper(input_files, template, output_name, entry_modifier = {}, entry_filter = None, entry_mapper = {}, entry_constructer = {},
               use_set = False, node_path = None, mode = Mode.aggregation):
    reader = Reader(template = template,
                    output_name = output_name,
                    entry_modifier = entry_modifier,
                    entry_filter = entry_filter,
                    entry_mapper = entry_mapper,
                    entry_constructer = entry_constructer,
                    use_set = use_set,
                    mode = mode)

    looper = Looper(file_names = input_files,
                    node_path = node_path,
                    reader = reader)

    return looper
