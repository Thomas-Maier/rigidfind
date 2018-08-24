
class Field(object):
    def __init__(self, field = ''):
        super(Field, self).__init__()
        self._payload = {'field': field}
        self.name = field

    def get(self):
        return self._payload

class Script(object):
    def __init__(self):
        super(Script, self).__init__()
        self._cache = set()
        self._script_string = ''

    def _get_script_string(self, operator, *fields):
        script_list = []
        for field in fields:
            if field in self._cache:
                script_list.append(field)
            else:
                script_list.append("doc['{}'].value".format(field))

        return '{}'.format(operator).join(script_list)

    def _operation(self, operator, *fields):
        script_string = '({})'.format(self._get_script_string(operator, *fields))
        self._cache.add(script_string)
        self._script_string = script_string

        return script_string

    def add(self, *fields):
        return self._operation('+', *fields)

    def subtract(self, *fields):
        return self._operation('-', *fields)

    def multiply(self, *fields):
        return self._operation('*', *fields)

    def divide(self, *fields):
        return self._operation('/', *fields)

    def zero_protect(self, script_string, default_value, *fields):
        protect_list = []
        for field in fields:
            protect_list.append("doc['{}'].value".format(field))    
        protect_string = '({}) ? {} : {}'.format(' && '.join(protect_list), script_string, default_value)
        self._script_string = protect_string

        return protect_string

    def string(self):
        return self._script_string


class ScriptField(Field, Script):
    def __init__(self, script_string = '', lang = 'expression', name = ''):
        super(ScriptField, self).__init__()
        self._script_string = script_string
        self._lang = lang
        self.name = name

    def _get_script_field(self):
        return {
            'script': {
                'inline': self._script_string,
                'lang': self._lang
            }
        }

    def get(self):
        self._payload = self._get_script_field()
        
        return super(ScriptField, self).get()
