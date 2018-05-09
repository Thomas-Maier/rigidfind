
import json

class Searcher:
    def __init__(self, instance, query, index, output = 'output.json'):
        self._instance = instance
        self._query = query
        self._index = index
        self._output = output

    def search(self):
        result = self._instance.search(index = self._index, body = self._query, size = 1)
        with open(self._output, 'w') as out_file:
            json.dump(result, out_file)
