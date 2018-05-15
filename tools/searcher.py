
import json

class Searcher:
    def __init__(self, instance, query, index, output = 'output.json', parse_output = True):
        self._instance = instance
        self._query = query
        self._index = index
        self._output = output
        self._parse_output = parse_output

    def search(self):
        result = self._instance.search(index = self._index, body = self._query, size = 1)
        output = {}
        if self._parse_output:
            for agg_name, agg_data in result['aggregations'].items():
                Searcher._parse(agg_name, agg_data, output)
        else:
            output = result
        with open(self._output, 'w') as out_file:
            json.dump(output, out_file)

    ## parsing probably needs to be more aware of what is happening in the query
    @staticmethod
    def _parse(agg_name, agg_data, output):
        output[agg_name] = {}
        data_name = 'buckets'
        if data_name not in agg_data:
            data_name = 'values'
        for entry in agg_data[data_name]:
            agg_key = entry['key']
            output[agg_name][agg_key] = {}
            count_name = 'doc_count'
            if count_name not in entry:
                count_name = 'value'
            count = entry[count_name]
            output[agg_name][agg_key]['count'] = count
            if len(entry) > 2:
                sub_agg_name = ''
                for key in entry.keys():
                    if key == 'doc_count' or key == 'key' or key = 'value': continue
                    sub_agg_name = key
                    break
                Searcher._parse(sub_agg_name, entry[sub_agg_name], output[agg_name][agg_key])
