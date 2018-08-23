
from __future__ import print_function
from enum import Enum


class Format(Enum):
    h5 = 0
    json = 1
    gzipjson = 2
    pkl = 3
    gzippkl = 4

def open_dict_file(file_name, file_type = Format.pkl):
    this_open = open
    if file_type == Format.gzipjson or file_type == Format.gzippkl:
        import gzip
        this_open = gzip.open
    mode = 'r'
    if file_type == Format.pkl or file_type == Format.gzippkl:
        mode = 'rb'
    with this_open(file_name, mode) as in_file:
        if file_type == Format.pkl or file_type == Format.gzippkl:
            import pickle
            return pickle.load(in_file)
        elif file_type == Format.json or file_type == Format.gzipjson:
            import json
            return json.load(in_file)
        else:
            print ('Not a valid json or pickle file, returning empty dict')
            return {}

def create_table(h5file, node_path, description):
    if node_path.startswith('/') or node_path.endswith('/'): node_path = node_path.strip('/')
    node_path = node_path.split('/')
    group = h5file.root
    for group_name in node_path[:len(node_path)-1]:
        group = h5file.create_group(group, group_name)

    return h5file.create_table(group, node_path[-1], description)
