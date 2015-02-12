from six import iteritems
from retrying import retry

from .utils import DslBase
from .field_old import InnerObject
from .connections import connections

class Properties(InnerObject, DslBase):
    def __init__(self, name):
        self._name = name
        super(Properties, self).__init__()

    @property
    def name(self):
        return self._name

@retry(wait_exponential_multiplier=4000, wait_exponential_max=60000)
def _save_mapping(conn, index, doc_type, body):
    if not conn.indices.exists(index=index):
        conn.indices.create(index=index, body={'mappings': body})
    else:
        conn.indices.put_mapping(index=index, doc_type=doc_type, body=body)

@retry(wait_exponential_multiplier=4000, wait_exponential_max=60000)
def _get_mapping(conn, index, doc_type):
    return conn.indices.get_mapping(index=index, doc_type=doc_type)

class Mapping(object):
    def __init__(self, name):
        self.properties = Properties(name)
        self._meta = {}

    @classmethod
    def from_es(cls, index, doc_type, using='default'):
        m = cls(doc_type)
        m.update_from_es(index, using)
        return m

    def save(self, index, using='default'):
        # TODO: analyzers, ...
        es = connections.get_connection(using)
        _save_mapping(es, index, self.doc_type, self.to_dict())

    def update_from_es(self, index, using='default'):
        es = connections.get_connection(using)
        raw = _get_mapping(es, index, self.doc_type)
        raw = raw[index]['mappings'][self.doc_type]

        for name, definition in iteritems(raw['properties']):
            self.field(name, definition)

        # metadata like _all etc
        for name, value in iteritems(raw):
            if name.startswith('_'):
                self.meta(name, **value)

    def update(self, mapping, update_only=False):
        for name in mapping:
            if update_only and name in self:
                # nested and inner objects, merge recursively
                if hasattr(self[name], 'update'):
                    self[name].update(mapping[name])
                continue
            self.field(name, mapping[name])

        if update_only:
            for name in mapping._meta:
                if name not in self._meta:
                    self._meta[name] = mapping._meta[name]
        else:
            self._meta.update(mapping._meta)

    def __contains__(self, name):
        return name in self.properties.properties

    def __getitem__(self, name):
        return self.properties.properties[name]

    def __iter__(self):
        return iter(self.properties.properties)

    @property
    def doc_type(self):
        return self.properties.name

    def field(self, name, field):
        self.properties.property(name, field)
        return self

    def meta(self, name, **kwargs):
        if not kwargs:
            if name in self._meta:
                del self._meta[name]
        else:
            self._meta[name] = kwargs
        return self

    def to_dict(self):
        ret = {}
        ret[self.doc_type] = {
            'properties': {}
        }
        for name, field in self.properties._params.get('properties').iteritems():
            ret[self.doc_type]['properties'][name] = field.mapping
        ret[self.doc_type].update(self._meta)
        return ret
