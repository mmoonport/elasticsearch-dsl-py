import re

from elasticsearch import Elasticsearch
from retrying import retry

from .search import Search
from .mapping import Mapping
from .fields import BaseField, DOC_META_FIELDS, META_FIELDS, FULL_META_FIELDS
from .connections import connections
from .exceptions import ValidationError
from .queue import Queue


@retry(stop_max_attemp_number=5, wait_fixed=3000)
def _save_document(conn, index, doc_type, body, extra):
    return conn.index(index=index, doc_type=doc_type, body=body, **extra)

@retry(stop_max_attemp_number=5, wait_fixed=3000)
def _delete_document(conn, index, doc_type, extra):
    return conn.delete(index=index, doc_type=doc_type, **extra)

@retry(stop_max_attemp_number=5, wait_fixed=3000)
def _get_document(es, index, doc_type, id, kwargs):
    return es.get(index=index, doc_type=doc_type, id=id, **kwargs)

@retry(wait_exponential_multiplier=4000, wait_exponential_max=60000)
def _drop_index(conn, index):
    return conn.indices.delete(index)

@retry(wait_exponential_multiplier=4000, wait_exponential_max=60000)
def _count_index(conn, index, doc_type):
    return conn.count(index=index, doc_type=doc_type)

class BulkInsert(object):
    def __init__(self, cls, index=None):
        self.doc_class = cls
        self.prev_index = self.doc_class.meta.index
        self.doc_class.meta.index = index or self.prev_index
        self.index = self.doc_class.meta.index

    def __enter__(self):
        self.doc_class._bulk = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.doc_class._bulk = False
        self.doc_class.meta.index = self.prev_index
        self.doc_class._bulk_queue._send(self.index)


class MetaDict(object):
    def __init__(self, name, bases, fields):
        meta = fields.pop('meta', None)
        self.index = meta.get('index', None) if meta else None
        self._using = meta.get('using', None) if meta else None
        self.bulk_size = meta.get('bulk_size', None) if meta else None
        self.doc_info = {}
        self.doc_type = meta.get('doc_type', re.sub(r'(.)([A-Z])', r'\1_\2', name).lower()) \
            if meta else re.sub(r'(.)([A-Z])', r'\1_\2', name).lower()

        self.mapping = meta.get('mapping', Mapping(self.doc_type)) if meta else Mapping(self.doc_type)

        # register all declared fields into the mapping
        for name, value in list(fields.iteritems()):
            if isinstance(value, BaseField):
                self.mapping.field(name, value)

    def __getitem__(self, item):
        return self.__dict__[item]


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
        new_class._bulk = False

        # document inheritance - include the fields from parents' mappings and
        # index/using values
        doc_fields = {}
        for b in bases:
            if hasattr(b, 'meta') and hasattr(b.meta, 'mapping'):
                new_class.meta.mapping.update(b.meta.mapping, update_only=True)
                new_class.meta._using = new_class.meta._using or b.meta._using
                new_class.meta.index = new_class.meta.index or b.meta.index
                new_class.meta.doc_type = new_class.meta.doc_type or b.meta.doc_type
                new_class.meta.bulk_size = new_class.meta.bulk_size or b.meta.bulk_size

            if hasattr(b, '_fields'):
                for field_name, field in b._fields.iteritems():
                    if isinstance(field, BaseField):
                        doc_fields[field_name] = field

        new_class._bulk_queue = Queue(index=new_class.meta.index,
                                      using=new_class.meta._using,
                                      limit=new_class.meta.bulk_size)

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
        self._data = {}
        self.meta.doc_info = {}
        errors = []
        for name, field in self._fields.iteritems():
            if name in kwargs.keys():
                setattr(self, name, field.to_python(kwargs.get(name)))
            else:
                setattr(self, name, field.to_python(field.default))


        for k in META_FIELDS:
            if '_' + k in kwargs.keys():
                if k == "type":
                    self.meta.doc_info['doc_type'] = kwargs['_{}'.format(k)]
                else:
                    self.meta.doc_info[k] = kwargs['_{}'.format(k)]


    def __setattr__(self, key, value):
        self._data[key] = value
        super(BaseDocument, self).__setattr__(key, value)

    @classmethod
    def drop(cls, index=None, using=None):
        es = connections.get_connection(using or cls.meta._using)
        try:
            resp = _drop_index(es, index or cls.meta.index)
            return resp
        except:
            return None

    @property
    def id(self):
        return self.meta.doc_info.get('id', None)

    @id.setter
    def id(self, id):
        self.meta.doc_info['id'] = id;

    @classmethod
    def init(cls, index=None, using=None):
        cls.meta.init(index, using)

    @classmethod
    def count(cls, using=None, index=None, doc_type=None):
        es = connections.get_connection(using or cls.meta._using)
        count = _count_index(es, index=(index or cls.meta.index), doc_type=(doc_type or cls.meta.doc_info.get('doc_type', cls.meta.name)))
        return count.get('count', 0)

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
            index = self.meta.doc_info.get('index', self.meta.index)
        if index is None:
            raise #XXX - no index
        # extract parent, routing etc from _meta
        doc_meta = dict((k, self.meta.doc_info.get(k)) for k in DOC_META_FIELDS if k in self.meta.doc_info.keys())
        doc_meta.update(kwargs)

        return _delete_document(es, index=index,
                                doc_type=self.meta.doc_info.get('doc_type', self.meta.name),
                                extra=doc_meta)


    @classmethod
    def get(cls, id, using=None, index=None, **kwargs):
        es = connections.get_connection(using or cls.meta._using)
        doc = _get_document(es, index=index or cls.meta.index,
            doc_type=cls.meta.name,
            id=id,
            **kwargs)
        return cls.from_es(doc)

    def save(self, using=None, index=None, bulk=False, flush=False, **kwargs):
        self.clean()
        self.validate()

        es = self._get_connection(using)
        if index is None:
            index = self.meta.doc_info.get('index', self.meta.index)
        if index is None:
            raise #XXX - no index
        # extract parent, routing etc from _meta
        self.meta.doc_info['index'] = index
        self.meta.doc_info['doc_type'] = self.meta.doc_info.get('doc_type', self.meta.name)
        doc_meta = dict((k, self.meta.doc_info.get(k)) for k in DOC_META_FIELDS if k in self.meta.doc_info.keys())
        doc_meta.update(kwargs)

        if bulk or self._bulk:
            self._bulk_queue.append(self, index)
            if flush:
                self._bulk_queue._send(index)
            return True

        meta = _save_document(es,
                              index=self.meta.doc_info['index'],
                              doc_type=self.meta.doc_info['doc_type'],
                              body=self.to_dict(),
                              extra=doc_meta)
        # update meta information from ES
        for k in META_FIELDS:
            if '_' + k in meta:
                if k == "type":
                    self.meta.doc_info['doc_type'] = meta['_{}'.format(k)]
                else:
                    self.meta.doc_info[k] = meta['_{}'.format(k)]
        # return True/False if the document has been created/updated
        return meta['created']


    def _get_connection(self, using=None):
        return connections.get_connection(using or self.meta._using)

    def to_dict(self):
        data = {}
        for key, value in self._data.iteritems():
            if key in self._fields.keys():
                value = getattr(self, key, None)
                data[key] = self._fields[key].to_python(value)
            else:
                data[key] = getattr(self, key, None)
        return data


    def to_es(self):
        self.meta.doc_info['index'] = self.meta.doc_info.get('index', self.meta.index)
        self.meta.doc_info['doc_type'] = self.meta.doc_info.get('doc_type', self.meta.name)
        doc = {"_source": self.to_dict()}
        doc_meta = dict(("_{}".format(k), self.meta.doc_info.get(k)) for k in FULL_META_FIELDS  if k in self.meta.doc_info.keys() and self.meta.doc_info[k] is not None)
        doc_meta['_type'] = doc_meta.pop('_doc_type')
        doc.update(doc_meta)
        return doc