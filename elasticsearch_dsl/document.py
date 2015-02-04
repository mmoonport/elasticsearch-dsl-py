from elasticsearch import Elasticsearch
import re
from elasticsearch_dsl import Search, Mapping
from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.exceptions import ValidationError
from elasticsearch_dsl.fields import BaseField


class MetaDict(object):
    def __init__(self, name, bases, fields):

        meta = fields.pop('meta', None)
        self.index = meta.get('index', None) if meta else None
        self._using = meta.get('using', None) if meta else None
        self.doc_type = meta.get('doc_type', re.sub(r'(.)([A-Z])', r'\1_\2', name).lower()) if meta else re.sub(r'(.)([A-Z])', r'\1_\2', name).lower()
        self.mapping = meta.get('mapping', Mapping(self.doc_type)) if meta else Mapping(self.doc_type)

        # register all declared fields into the mapping
        for name, value in list(fields.iteritems()):
            if isinstance(value, BaseField):
                self.mapping.field(name, value)

    def update(self, meta):
        pass

    @property
    def name(self):
        return self.mapping.properties.name

class BaseDocumentMeta(type):

    def __new__(cls, name=None, bases=None, fields=None):
        super_new = super(BaseDocumentMeta, cls).__new__
        if name.startswith('None'):
            return None

        new_class = super_new(cls, name, bases, fields)
        new_class.meta = MetaDict(name, bases, fields)
        new_class.meta._using = Elasticsearch(hosts='es.xocur.com:9200')

        # document inheritance - include the fields from parents' mappings and
        # index/using values
        doc_fields = {}
        for b in bases:
            if hasattr(b, 'meta') and hasattr(b.meta, 'mapping'):
                new_class.meta.mapping.update(b.meta.mapping, update_only=True)
                new_class.meta._using = new_class.meta._using or b.meta._using
                new_class.meta.index = new_class.meta.index or b.meta.index
                new_class.meta.doc_type = new_class.meta.doc_type or b.meta.doc_type

            if hasattr(b, '_fields'):
                for field_name, field in b._fields.iteritems():
                    if isinstance(field, BaseField):
                        doc_fields[field_name] = field

        for field_name, field in fields.iteritems():
            if isinstance(field, BaseField):
                doc_fields[field_name] = field

        new_class._fields = doc_fields

        # if 'query' not in dir(new_class):
        new_class.query = Search(
            using=new_class.meta._using,
            index=new_class.meta.index,
            doc_type={new_class.meta.doc_type: new_class.from_es})
        return new_class


class BaseDocument(object):
    __metaclass__ = BaseDocumentMeta

    def __init__(self, **kwargs):
        errors = []
        for name, field in self._fields.iteritems():
            if name in kwargs.keys():
                setattr(self, name, field.to_python(kwargs.get(name)))
            else:
                setattr(self, name, field.to_python(field.default))

    @classmethod
    def from_es(cls, hit):
        # don't modify in place
        doc = hit.copy()
        doc.update(doc.pop('_source'))
        return cls(id=doc.pop('_id'), **doc)

    def validate(self):
        errors = []
        for name, field in self._fields.iteritems():
            try:
                value = getattr(self, name, None)
                field.validate(value)
            except ValidationError as e:
                errors.append('{} {}'.format(name, e.message))

        if errors:
            raise ValidationError('; '.join(errors))

    def save(self):
        self.validate()


    def _get_connection(self, using=None):
        return connections.get_connection(using or self.meta._using)


    def to_dict(self):
        data = {}
        for name, field in self._fields.iteritems():
            value = getattr(self, name, None)
            data[name] = field.to_python(value)
        return data