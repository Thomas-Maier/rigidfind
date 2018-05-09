
class Query:
    def __init__(self, fields = None, must = None, must_not = None, aggregations = None):
        self._fields = fields
        self._must = must
        self._must_not = must_not
        self._aggregations = aggregations

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
            query['_source'] = self._fields
        if self._must:
            for must_filter in self._must:
                query['query']['bool']['must'].append(must_filter.get())
        if self._must_not:
            for must_not_filter in self._must_not:
                query['query']['bool']['must_not'].append(must_not_filter.get())
        if self._aggregations:
            query['size'] = 0
            query['aggs'] = {}
            for agg in self._aggregations:
                query['aggs'].update(agg.get())

        return query
