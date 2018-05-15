
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

    @staticmethod
    def _parse(agg_name, agg_data, output):
        output[agg_name] = {}
        for entry in agg_data['buckets']:
            agg_key = entry['key']
            output[agg_name][agg_key] = {}
            count = entry['doc_count']
            output[agg_name][agg_key]['count'] = count
            if len(entry) > 2:
                sub_agg_name = ''
                for key in entry.keys():
                    if key == 'doc_count' or key == 'key': continue
                    sub_agg_name = key
                    break
                Searcher._parse(sub_agg_name, entry[sub_agg_name], output[agg_name][agg_key])
