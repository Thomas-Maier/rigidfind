
from elasticsearch import Elasticsearch
from elasticsearch import helpers
import gzip
import json
from sys import stdout
try:
    import tables
except ImportError:
    pass


class Scanner:
    def __init__(self, instance, query = None, index = None, index_filter = None, output_folder = None, entry_class = None,
                 table_path = None, entry_map = None, entry_default = None, entry_modifier = None, entry_filter = None, entry_digger = None,
                 debug = False):
        self._debug_flag = debug
        self._instance = instance
        self._query = query
        self._indices = self.get_indices(index, index_filter)
        self._output_folder = output_folder
        self._entry_default = entry_default
        self._entry_modifier = entry_modifier
        self._entry_filter = entry_filter
        self._entry_map = entry_map
        self._entry_digger = entry_digger
        ## H5 File
        self._entry_class = entry_class
        if self._entry_class is not None:
            if table_path is None:
                print 'ERROR: Please specify a table path'
                raise SystemExit
            self._table_path = self._parse_table_path(table_path)
            self._columns_to_check = set([l for l in entry_class.columns.keys() if entry_class.columns[l].kind == 'string'])
        self._bar_mode = True
        try:
            from tqdm import tqdm
        except ImportError:
            self._bar_mode = False

    def scan(self):
        self._debug('Start scanning indices: {}'.format(self._indices))
        if self._entry_class is not None:
            self._scan_h5()
        else:
            self._scan_json()

    def _scan_h5(self):
        filters = tables.Filters(complevel = 1, complib = 'lzo')
        if self._bar_mode:
            from tqdm import tqdm
            iter_tuple = enumerate(tqdm(self._indices, bar_format = '{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}'))
        else:
            iter_tuple = enumerate(self._indices)
        for i, index in iter_tuple:
            if not self._bar_mode:
                stdout.write('\r({}/{}) processing {}'.format(i+1, len(self._indices), index))
                stdout.flush()
            h5file = tables.open_file('{}/{}.h5'.format(self._output_folder, index), mode = 'w', title = '{0}'.format(index), filters = filters)
            table = self._create_table(h5file)
            row = table.row
            ## Scan ES index with python helpers module
            result = helpers.scan(self._instance, query = self._query, index = index, size = 10000)
            for hit in result:
                self._debug(hit)
                source = {}
                for col_name in table.colnames:
                    entry = self._get_entry(hit['_source'], col_name)
                    source[col_name] = entry
                ## Apply filters
                if self._filter(source): continue
                for key in source:
                    self._check_entry(source[key], key)
                    row[key] = source[key]
                row.append()
            table.flush()
            h5file.close()
        if not self._bar_mode:
            stdout.write('\n')

    def _scan_json(self):
        if self._bar_mode:
            from tqdm import tqdm
            iter_tuple = enumerate(tqdm(self._indices, bar_format = '{percentage:3.0f}%|{bar}| {n_fmt}/{total_fmt}'))
        else:
            iter_tuple = enumerate(self._indices)
        for i, index in iter_tuple:
            if not self._bar_mode:
                stdout.write('\r({}/{}) processing {}'.format(i+1, len(self._indices), index))
                stdout.flush()
            output = []
            ## Scan ES index with python helpers module
            result = helpers.scan(self._instance, query = self._query, index = index, size = 10000)
            for hit in result:
                self._debug(hit)
                source = {}
                for key in hit['_source']:
                    source[key] = self._get_entry(hit['_source'], key)
                ## Apply filters
                if self._filter(source): continue
                output.append(source)
            ## Write output
            out_file = gzip.open('{}/{}.json.gz'.format(self._output_folder, index), 'w')
            json.dump(output, out_file)
            out_file.close()
        if not self._bar_mode:
            stdout.write('\n')

    def get_indices(self, index, index_filter = None):
        indices = self._instance.cat.indices(index = index, h = 'index', request_timeout = 600).split('\n')
        ## Make sure that we don't have an empty entry
        indices = [l for l in indices if l]
        if index_filter: indices = [l for l in indices if index_filter(l)]

        return sorted(indices)

    def _get_entry(self, source, key_entry):
        key = key_entry
        if self._entry_map is not None and key_entry in self._entry_map:
            key = self._entry_map[key]
        if self._entry_default is not None and key_entry in self._entry_default:
            if key not in source or source[key] is None:
                source[key] = self._entry_default[key_entry]
        if self._entry_digger is not None and key_entry in self._entry_digger:
            dig_entry = self._dig_entry(source, key_entry, key, self._entry_digger)
            if dig_entry is not None: source[key] = dig_entry
        if self._entry_modifier is not None and key_entry in self._entry_modifier:
            if source[key] is not None:
                source[key] = self._entry_modifier[key_entry](source[key])

        return source[key]

    def _dig_entry(self, source, key, origin_key, digger):
        parent_key = digger[key]
        if isinstance(parent_key, dict):
            parent_key = parent_key.keys()[0]
            return self._dig_entry(source[parent_key], parent_key, origin_key, digger[key])

        if origin_key in source[parent_key]:
            return source[parent_key][origin_key]
        else:
            return None

    def _check_entry(self, entry, col_name):
        if col_name in self._columns_to_check and len(entry) > self._entry_class.columns[col_name].itemsize:
            print 'WARNING: entry "{}" for column "{}" is too long, max itemsize is {}!'.format(entry, col_name, self._entry_class.columns[col_name].itemsize)

    def _filter(self, source):
        if self._entry_filter is not None:
            for key in source:
                if key not in self._entry_filter: continue
                if self._entry_filter[key](source[key]): return True

        return False

    def _parse_table_path(self, table_path):
        if table_path.startswith('/') or table_path.endswith('/'): table_path = table_path.strip('/')

        return table_path.split('/')

    def _create_table(self, h5file):
        group = h5file.root
        for group_name in self._table_path[:len(self._table_path)-1]:
            group = h5file.create_group(group, group_name)

        return h5file.create_table(group, self._table_path[-1], self._entry_class)

    def print_debug(self):
        print 'Indices:', self._indices
        print 'Query:', self._query
        print 'Output folder:', self._output_folder
        print 'Entry defaults:', self._entry_default
        print 'Entry modifier:', self._entry_modifier
        print 'Entry filter:', self._entry_filter
        if self._entry_class is not None: print 'Columns to check:', self._columns_to_check

    def _debug(self, message):
        if not self._debug_flag:
            return
        print message
