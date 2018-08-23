#!/usr/bin/env python

from collections import OrderedDict
from enum import Enum
from helper import Format, create_table


class Mode(Enum):
    aggregation = 0
    skimming = 1


class Entry:
    def __init__(self, entry_modifier = {}, entry_mapper = {}):
        self.entry = None
        self._entry_modifier = entry_modifier
        self._entry_mapper = entry_mapper
        self._cache = {}

    def __getitem__(self, key):
        ## If we have the value already cached, return it
        if key in self._cache:
            return self._cache[key]
        ## Map input key to the one in the input file
        key_mapped = key
        if key in self._entry_mapper:
            key_mapped = self._entry_mapper[key]
        value = self.entry[key_mapped]
        ## Modify value if a modifier for this key was set
        if key in self._entry_modifier:
            try:
                value = self._entry_modifier[key](value)
            except:
                print 'Could not properly apply modifier for key "{0}" and value "{1}"'.format(key, value)
                raise
        ## Cache value
        self._cache[key] = value

        return value

    def __repr__(self):
        return self._cache

    def cache(self, field):
        self[field]

    def load(self, input_entry):
        ## Clear cache
        self._cache.clear()
        ## Set new entry data
        self.entry = input_entry


class Reader:
    def __init__(self, template, output_name = None, entry_modifier = {}, entry_filter = None, entry_mapper = {}, use_set = False, mode = Mode.aggregation):
        self._mode = mode
        self._template = template
        self._payload = {}
        if self._mode == Mode.skimming:
            self._payload = []
            self._output = None
        self._entry_modifier = entry_modifier
        self._entry_filter = entry_filter
        self._entry_mapper = entry_mapper
        self._output_name = output_name
        self._output_format = Format.pkl
        self._use_set = use_set
        if self._use_set:
            self._output_format = Format.pkl

    def get_output_name(self):
        return self._output_name

    def _set_output(self, data):
        import tables
        ## If already set, do nothing
        if self._output is not None:
            return
        self._output_format = Format.h5
        filters = data.filters
        node_path = data._v_pathname
        description = data.description
        self._output = tables.open_file(self._output_name, 'w', filters = filters)
        self._payload = create_table(self._output, node_path, description)

    def get_data(self, data, input_format):
        if self._payload is None:
            print 'Reader is not properly configured, please check...'
            raise SystemExit
        ## If input is h5 file and it's just skimming, prepare output
        if self._mode == Mode.skimming and input_format == Format.h5:
            self._set_output(data)
        ## Retrieve entries
        self._retrieve_entries(data, input_format)

    def write_output(self):
        if self._output_format == Format.h5:
            self._payload.flush()
            self._output.close()
        else:
            with open(self._output_name, 'w') as outFile:
                if self._output_format == Format.pkl:
                    import pickle
                    pickle.dump(self._payload, outFile)
                else:
                    import json
                    json.dump(self._payload, outFile)

    def _retrieve_entries(self, data, input_format):
        iter_obj = None
        if input_format == Format.h5:
            iter_obj = data.iterrows()
        else:
            iter_obj = data
        ## Make entry object
        entry = Entry(entry_modifier = self._entry_modifier, entry_mapper = self._entry_mapper)
        for input_entry in iter_obj:
            ## Load current entry data
            entry.load(input_entry)
            ## Filter entry
            if self._entry_filter is not None and not self._entry_filter.keep(entry): continue
            ## Add data
            if self._mode == Mode.aggregation:
                self._add_entry(self._payload, self._template, entry)
            else:
                self._fill_entry(entry)

    def _add_entry(self, data, data_template, entry):
        if isinstance(data_template, dict):
            for key, template_val in data_template.items():
                val = entry[key]
                if val is None: continue
                if isinstance(template_val, dict):
                    if val not in data: data[val] = {}
                elif isinstance(template_val, int):
                    ## Using list here since integer is not mutable and our recursion magic wouldn't work
                    if val not in data: data[val] = [template_val]
                else:
                    if val not in data:
                        if self._use_set:
                            data[val] = set()
                        else:
                            data[val] = []
                self._add_entry(data[val], data_template[key], entry)
        elif isinstance(data_template, list):
            sub_dict = {}
            for template_entry in data_template:
                try:
                    sub_dict[template_entry] = entry[template_entry]
                except:
                    print 'Something went wrong while adding entries for template entry: {0}'.format(template_entry)
                    raise
            data.append(sub_dict)
        elif isinstance(data_template, int):
            data[0] += 1
        else:
            val = None
            try:
                val = entry[data_template]
            except:
                print 'Something went wrong while adding entries for template: {0}'.format(data_template)
                raise
            if val is not None:
                if self._use_set:
                    data.add(val)
                else:
                    data.append(val)

    def _fill_entry(self, entry):
        if self._output_format == Format.h5:
            for field in self._payload.colnames:
                self._payload.row[field] = entry[field]
            self._payload.row.append()
        else:
            sub_dict = {}
            for field in entry.entry:
                sub_dict[field] = entry[field]
            self._payload.append(sub_dict)


class Filter:
    def __init__(self, Whitelists = {}, Blacklists = {}, Filters = {}):
        self.whitelists = OrderedDict()
        self.whitelists.update(Whitelists)
        self.blacklists = OrderedDict()
        self.blacklists.update(Blacklists)
        self.filters = []
        for func, fields in Filters:
            ## Check if fields is already a list or tuple
            if not isinstance(fields, list) and not isinstance(fields, tuple):
                fields = [fields]
            self.filters.append((func, fields))

    def keep(self, entry):
        ## Check whitelists
        for field in self.whitelists:
            if entry[field] in self.whitelists[field]: continue
            return False
        ## Check blacklists
        for field in self.blacklists:
            if entry[field] not in self.blacklists[field]: continue
            return False
        ## Check filters
        for func, fields in self.filters:
            ## Cache relevant fields in the entry
            for field in fields:
                entry.cache(field)
            if func(entry): continue
            return False

        return True

    def set_whitelist(self, whitelist, field):
        self.whitelists[field] = set(whitelist)

    def set_blacklist(self, blacklist, field):
        self.blacklists[field] = set(blacklist)

    def set_filter(self, function, fields):
        self.filters.append((function, fields))
