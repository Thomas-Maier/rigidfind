
## Should also allow for 'scripts' entry instead of 'field'

class Aggregation(object):
    def __init__(self, field):
        self.name = ''
        self.subs = []
        self.payload = {}
        self._field = field

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
        super(TermAggregation, self).__init__(field)
        self.name = field.name
        self._size = size
        self._order = order
        self.payload = self._get_term_aggregation()

    def _get_term_aggregation(self):
        agg_dict = {
            'terms': {
                'size': self._size,
                'order': {
                    '_term': self._order
                }
            }
        }
        agg_dict['terms'].update(self._field.get())

        return agg_dict

    
class PercentileAggregation(Aggregation):
    def __init__(self, field, percentiles = [50]):
        super(PercentileAggregation, self).__init__(field)
        self.name = field.name
        self._percentiles = percentiles
        self.payload = self._get_percentile_aggregation()

    def _get_percentile_aggregation(self):
        agg_dict = {
            'percentiles': {
                'percents': self._percentiles,
                'keyed': False
            }
        }
        agg_dict['percentiles'].update(self._field.get())

        return agg_dict
