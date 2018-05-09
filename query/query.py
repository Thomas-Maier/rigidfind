
class Query:
    def __init__(self, fields = None, must = None, must_not = None):
        self._fields = fields
        self._must = must
        self._must_not = must_not

    def get(self):
        query = {
            'query': {
                'bool': {
                    'must': [],
                    'must_not': []
                }
            }
        }
        if self._fields:
            query['_source'] = fields
        if self._must:
            for must_filter in self._must:
                query['query']['bool']['must'].append(must_filter.get())
        if self._must_not:
            for must_not_filter in self._must_not:
                query['query']['bool']['must_not'].append(must_not_filter.get())
