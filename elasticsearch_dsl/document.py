from elasticsearch_dsl.result import ResultMeta
from elasticsearch_dsl.utils import _count_index, _delete_document, _get_document, _save_document, _drop_index, \
    _make_doc_type_from_name

from .search import Search
from .mapping import Mapping
from .fields import BaseField, DOC_META_FIELDS, META_FIELDS, FULL_META_FIELDS
from .connections import connections
from .exceptions import ValidationError, ReadOnlyException
from .queue import Queue

class BulkInsert(object):
    def __init__(self, cls, index=None):
        self.doc_class = cls
        self.prev_index = self.doc_class._d.index
        self.doc_class._d.index = index or self.prev_index
        self.index = self._d.index

    def __enter__(self):
        self.doc_class._d._bulk = True

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.doc_class._d._bulk = False
        self.doc_class._d.index = self.prev_index
        self.doc_class._queue._send(self.index)


class DocMapping(object):
    def __init__(self, name, bases, fields):
        meta = fields.pop('meta', {})
        self.index = meta.get('index', None)
        self._using = meta.get('using', None)
        self._bulk = meta.get('bulk', None)
        self._bulk_size = meta.get('bulk_size', None)
        self._read_only = meta.get('read_only', False)
        doc_type = meta.get('doc_type', _make_doc_type_from_name(name))
        self.mapping = meta.get('mapping', Mapping(doc_type))


        for b in bases:
            if hasattr(b, '_d') and hasattr(b._d, 'mapping'):
                self.mapping.update(b._d.mapping, update_only=True)
                self._using = self._using or b._d._using
                self.index = self.index or b._d.index
                self._bulk = self._bulk or b._d._bulk
                self._bulk_size = self._bulk_size or b._d._bulk_size
                self._read_only = self._read_only or b._d._read_only

        # register all declared fields into the mapping
        for field_name, value in list(fields.iteritems()):
            if isinstance(value, BaseField):
                self.mapping.field(field_name, value)

    @property
    def doc_type(self):
        return self.mapping.properties.name

    @property
    def using(self):
        return self._using or 'default'

    def init(self, index=None, using=None):
        self.mapping.save(index or self.index, using=using or self._using)

    def refresh(self, index=None, using=None):
        self.mapping.update_from_es(index or self.index, using=using or self._using)


class BaseDocumentMeta(type):

    def __new__(cls, name=None, bases=None, fields=None):
        super_new = super(BaseDocumentMeta, cls).__new__
        fields['_d'] = DocMapping(name, bases, fields)
        new_class = super_new(cls, name, bases, fields)
        new_class._queue = Queue(index=fields['_d'].index,
                                 using=fields['_d'].using,
                                 limit=fields['_d']._bulk_size or 100)

        new_class._fields = {}
        for b in bases:
            if hasattr(b, '_fields'):
                new_class._fields.update(b._fields)
        new_class._fields.update({field_name: field for field_name, field in fields.iteritems() if isinstance(field, BaseField)})


        #if 'query' not in dir(new_class):
        new_class.query = Search(
            using=fields['_d'].using,
            index=fields['_d'].index,
            doc_type={fields['_d'].doc_type: new_class.from_es})
        return new_class


class BaseDocument(object):
    __metaclass__ = BaseDocumentMeta
    def __init__(self, id=None, **kwargs):
        self._data = {}
        for name, field in self._fields.iteritems():
            if name in kwargs.keys():
                setattr(self, name, field.to_python(kwargs.get(name)))
            else:
                setattr(self, name, field.to_python(field.default))
        meta = {'id': id}
        for k in list(kwargs):
            if k.startswith('_') and k[1:] in META_FIELDS:
                meta[k] = kwargs.pop(k)
        self._meta = ResultMeta(meta)


    def __setattr__(self, key, value):
        if key not in ['_data', '_meta']:
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
        return self._meta.id

    @id.setter
    def id(self, id):
        self._meta.id = id

    @classmethod
    def init(cls, index=None, using=None):
        cls._d.init(index, using)

    @classmethod
    def count(cls, using=None, index=None, doc_type=None):
        es = connections.get_connection(using or cls._d._using)
        count = _count_index(es, index=index or cls._d.index, doc_type=doc_type or cls._d.doc_type)
        return count.get('count', 0)

    @classmethod
    def from_es(cls, hit):
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

    def clean(self):
        pass

    def delete(self, using=None, index=None, **kwargs):
        es = self._get_connection(using)
        if index is None:
            index = getattr(self._meta, 'index', self._d.index)
        if index is None:
            raise #XXX - no index
        # extract parent, routing etc from _meta
        doc_meta = dict((k, self._meta[k]) for k in DOC_META_FIELDS if k in self._meta)
        doc_meta.update(kwargs)

        return _delete_document(es, index=index,
                                doc_type=getattr(self._meta, 'doc_type', self._d.doc_type),
                                extra=doc_meta)


    @classmethod
    def get(cls, id, using=None, index=None, **kwargs):
        es = connections.get_connection(using or cls._d._using)
        doc = _get_document(es, index=index or cls._d.index,
            doc_type=cls._d.doc_type,
            id=id,
            **kwargs)
        return cls.from_es(doc)

    def save(self, using=None, index=None, bulk=False, flush=False, force=False, **kwargs):
        if not self._d._read_only or self._d._read_only and force:
            self.clean()
            self.validate()

            es = self._get_connection(using)
            if index is None:
                index = getattr(self._meta, 'index', self._d.index)
            if index is None:
                raise #XXX - no index

            # extract parent, routing etc from _meta
            doc_meta = dict((k, self._meta[k]) for k in DOC_META_FIELDS if k in self._meta)
            doc_meta.update(kwargs)
            if bulk or self._d._bulk:
                self._queue.append(self, index)
                if flush:
                    self._queue._send(index)
                return True

            meta = _save_document(es,
                                  index=index,
                                  doc_type=self._d.doc_type,
                                  body=self.to_dict(),
                                  extra=doc_meta)
            # update meta information from ES
            for k in META_FIELDS:
                if '_{}'.format(k) in meta:
                    setattr(self._meta, k, meta['_{}'.format(k)])
            # return True/False if the document has been created/updated
            return meta['created']
        raise ReadOnlyException('This document is read only. To force save set force=True in save call')


    def _get_connection(self, using=None):
        return connections.get_connection(using or self._d._using)

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
        doc = {"_source": self.to_dict()}
        doc_meta = dict((k, self._meta[k]) for k in FULL_META_FIELDS if k in self._meta)
        doc_meta['_type'] = getattr(self._meta, 'doc_type', self._d.doc_type)
        doc.update(doc_meta)
        return doc