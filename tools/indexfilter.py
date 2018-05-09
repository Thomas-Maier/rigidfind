
class IndexFilter:
    def __init__(self, gte, lte, date_format):
        self._date_gte = datetime.datetime(**gte)
        self._date_lte = datetime.datetime(**lte)
        self._date_format = date_format

    def __call__(self, val):
        return self._date_check(val)

    def _date_check(self, val):
        try:
            return (self._date_gte <= datetime.datetime.strptime(val, self._date_format) <= self._date_lte)
        except ValueError:
            return False
