
class Aggregation(object):
    def __init__(self):
        self.name = ''
        self.subs = []
        self.payload = {}

    def get(self):
        # query = {'aggs': self.payload}
        # Aggregation._get(self.subs, query['aggs'])
        query = {}
        Aggregation._get(self, query)
        
        return query

    def add(self, agg):
        self.subs.append(agg)

    @staticmethod
    def _get(agg, query):
        # if not 'aggs' in query: query['aggs'] = {}
        name = agg.name
        query[name] = agg.payload
        subs = agg.subs
        if agg.subs: query[name]['aggs'] = {}
        for sub in agg.subs:
            Aggregation._get(sub, query[name]['aggs'])
            # sub_payload = sub.payload
            # query[name]['aggs'].update(sub_payload)
            # if sub.subs:
            #     _get(sub.subs, query[name]['aggs'])


class TermAggregation(Aggregation):
    def __init__(self, field, size, order = 'desc'):
        super(TermAggregation, self).__init__()
        self._field = field
        self.name = field
        self._size = size
        self._order = order
        self.payload = self._get_term_aggregation()

    def _get_term_aggregation(self):
        return {
            'terms': {
                'field': self._field,
                'size': self._size,
                'order': {
                    '_term': self._order
                }
            }
        }
