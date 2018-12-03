
class Filter:
    def __init__(self):
        return

    def _get_match_all(self):
        return {
            'match_all': {}
        }

    def get(self):
        return self._get_match_all()
    

class TermFilter(Filter):
    def __init__(self, field, value):
        self.field = field
        self.value = value
        
    def _get_match_term(self):
        return {
            'match_phrase': {
                self.field: {
                    'query': self.value
                }
            }
        }

    def get(self):
        return self._get_match_term()


class QueryFilter(Filter):
    def __init__(self, query):
        self._query = query
        
    def _get_match_query(self):
        return {
            'query_string': {
                'query': self._query,
                'analyze_wildcard': True,
                'lowercase_expanded_terms': False
            }
        }

    def get(self):
        return self._get_match_query()

    
class RangeFilter(Filter):
    def __init__(self, field, gte, lte, format = 'epoch_second', date_format = '%Y-%m-%dT%H:%M:%S'):
        self._field = field
        from .utils import date_to_unix
        self._gte = gte if not isinstance(gte, str) else date_to_unix(gte, date_format = date_format)
        self._lte = lte if not isinstance(lte, str) else date_to_unix(lte, date_format = date_format)
        self._format = format

    def _get_match_range(self):
        return {
            'range': {
                self._field: {
                    'gte': self._gte,
                    'lte': self._lte,
                    'format': self._format
                }
            }
        }

    def get(self):
        return self._get_match_range()
    
