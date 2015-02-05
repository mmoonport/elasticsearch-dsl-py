import re

from elasticsearch import Elasticsearch

from .search import Search
from .mapping import Mapping
from .fields import BaseField, DOC_META_FIELDS, META_FIELDS
from .connections import connections
from .exceptions import ValidationError

class MetaDict(object):
    def __init__(self, name, bases, fields):

        meta = fields.pop('meta', None)
        self.index = meta.get('index', None) if meta else None
        self._using = meta.get('using', None) if meta else None

        self.doc_type = meta.get('doc_type', re.sub(r'(.)([A-Z])', r'\1_\2', name).lower()) \
            if meta else re.sub(r'(.)([A-Z])', r'\1_\2', name).lower()

        self.mapping = meta.get('mapping', Mapping(self.doc_type)) if meta else Mapping(self.doc_type)

        # register all declared fields into the mapping
        for name, value in list(fields.iteritems()):
            if isinstance(value, BaseField):
                self.mapping.field(name, value)

    def __iter__(self):
        for attr in dir(self):
            if not attr.startswith('__'):
                yield attr

    def init(self, index=None, using=None):
        self.mapping.save(index or self.index, using=using or self._using)

    def refresh(self, index=None, using=None):
        self.mapping.update_from_es(index or self.index, using=using or self._using)

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
        new_class._data = {}

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


        # update meta information from ES
        for k in META_FIELDS:
            if '_' + k in kwargs.keys():
                if k == "type":
                    setattr(self.meta, "doc_type", kwargs['_' + k])
                else:
                    setattr(self.meta, k, kwargs['_' + k])

    def __setattr__(self, key, value):
        self._data[key] = value
        super(BaseDocument, self).__setattr__(key, value)

    @property
    def id(self):
        return getattr(self.meta, 'id', None)

    @id.setter
    def id(self, id):
        self.meta.id = id;

    @classmethod
    def init(cls, index=None, using=None):
        cls.meta.init(index, using)

    @classmethod
    def from_es(cls, hit):
        doc = hit.copy()
        doc.update(doc.pop('_source'))
        return cls(**doc)

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

    def clean(self):
        pass

    def delete(self, using=None, index=None, **kwargs):
        es = self._get_connection(using)
        if index is None:
            index = self.meta.index
        if index is None:
            raise #XXX - no index
        # extract parent, routing etc from _meta
        doc_meta = dict((k, getattr(self.meta, k)) for k in DOC_META_FIELDS if k in self.meta)
        doc_meta.update(kwargs)
        return es.delete(
            index=index,
            doc_type=self.meta.name,
            **doc_meta
        )

    @classmethod
    def get(cls, id, using=None, index=None, **kwargs):
        es = connections.get_connection(using or cls.meta._using)
        doc = es.get(
            index=index or cls.meta.index,
            doc_type=cls.meta.name,
            id=id,
            **kwargs
        )
        return cls.from_es(doc)

    def save(self, using=None, index=None, **kwargs):
        self.clean()
        self.validate()

        es = self._get_connection(using)
        if index is None:
            index = self.meta.index
        if index is None:
            raise #XXX - no index
        # extract parent, routing etc from _meta
        doc_meta = dict((k, self.meta[k]) for k in DOC_META_FIELDS if k in self.meta)
        doc_meta.update(kwargs)
        meta = es.index(
            index=index,
            doc_type=self.meta.name,
            body=self.to_dict(),
            **doc_meta
        )
        # update meta information from ES
        for k in META_FIELDS:
            if '_' + k in meta:
                if k == "type":
                    setattr(self.meta, "doc_type", meta['_' + k])
                else:
                    setattr(self.meta, k, meta['_' + k])
        # return True/False if the document has been created/updated
        return meta['created']

    def _get_connection(self, using=None):
        return connections.get_connection(using or self.meta._using)

    def to_dict(self):
        data = {}
        for key, value in self._data.iteritems():
            if not key.startswith('__'):
                if key in self._fields.keys():
                    value = getattr(self, key, None)
                    data[key] = self._fields[key].to_python(value)
                else:
                    data[key] = getattr(self, key, None)
        return data