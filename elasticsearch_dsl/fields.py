import decimal
import datetime
import dateutil
from elasticsearch import Elasticsearch
import time
from elasticsearch_dsl import Search
from elasticsearch_dsl.exceptions import ValidationError

__author__ = 'mmoon'

DOC_META_FIELDS = frozenset((
    'id', 'parent', 'routing', 'timestamp', 'ttl', 'version', 'version_type'
))

META_FIELDS = frozenset((
    # Elasticsearch metadata fields, except 'type'
    'index', 'using', 'score',
)).union(DOC_META_FIELDS)

FULL_META_FIELDS = frozenset((
    'doc_type',
)).union(META_FIELDS)


class BaseField(object):
    def __init__(self, required=False, default=None, **kwargs):
        self.required = required
        self.default = default
        self._meta = getattr(self, '_meta', {})
        for key, value in kwargs.iteritems():
            if key in META_FIELDS:
                self._meta[key] = value


    def to_python(self, value):
        return value

    def validate(self, value):
        if not value and self.required:
            raise ValidationError('field is required')

    @property
    def mapping(self):
        return self._meta


class BooleanField(BaseField):

    def to_python(self, value):
        try:
            value = bool(value)
        except ValueError:
            pass
        return value

    def validate(self, value):
        if not isinstance(value, bool):
            raise ValidationError('field only accepts boolean values')




class StringField(BaseField):
    def __init__(self, min_length=None, max_length=None, **kwargs):
        self.min_length = min_length
        self.max_length = max_length
        self._meta = {
            'type': 'string'
        }
        super(StringField, self).__init__(**kwargs)

    def to_python(self, value):
        if value:
            return str(value)
        return None



    def validate(self, value):
        super(StringField, self).validate(value)
        if isinstance(value, (list, tuple, set)):
            value = ','.join(value)
        if not isinstance(value, basestring):
            raise ValidationError('field must be string')


class ListField(StringField):
    def __init__(self, **kwargs):
        super(ListField, self).__init__(**kwargs)

    def to_python(self, value):
        if value:
            if isinstance(value, str):
                return value.split(',')
            elif isinstance(value, (list, tuple, set)):
                return value
        return None

    def validate(self, value):
        if not value and self.required:
            raise ValidationError('field is required')

        if isinstance(value, (list, tuple, set)):
            try:
                ','.join(value)
            except Exception, e:
                raise ValidationError('field failed to convert list to string')
        elif not isinstance(value, str):
            raise ValidationError('field must be a valid list or string of list')


class IntField(BaseField):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value = min_value
        self.max_value = max_value
        super(IntField, self).__init__(**kwargs)

    def to_python(self, value):
        if value:
            return int(value)
        return None

    def validate(self, value):
        super(IntField, self).validate(value)
        if not isinstance(value, int):
            raise ValidationError('field must be int')

        if self.min_value is not None and value < self.min_value:
            raise ValidationError('field value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('field value is too large')


class FloatField(BaseField):
    def __init__(self, min_value=None, max_value=None, **kwargs):
        self.min_value = min_value
        self.max_value = max_value

        super(FloatField, self).__init__(**kwargs)

    def to_python(self, value):
        if value:
            return int(value)
        return None

    def validate(self, value):
        super(FloatField, self).validate(value)
        if isinstance(value, int):
            value = float(value)

        if not isinstance(value, float):
            raise ValidationError('field only accepts float')

        if self.min_value is not None and value < self.min_value:
            raise ValidationError('field value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('field value is too large')


class DecimalField(BaseField):
    def __init__(self, min_value=None, max_value=None, precision=2,
                 rounding=decimal.ROUND_HALF_UP, **kwargs):
        self.min_value = min_value
        self.max_value = max_value
        self.precision = decimal.Decimal(".%s" % ("0" * precision))
        self.rounding = rounding

        super(DecimalField, self).__init__(**kwargs)

    def to_python(self, value):
        if value is None:
            return value
        try:
            value = decimal.Decimal("{}".format(value))
        except decimal.InvalidOperation:
            return value
        return value.quantize(self.precision, rounding=self.rounding)

    def validate(self, value):
        if not isinstance(value, decimal.Decimal):
            if not isinstance(value, basestring):
                value = unicode(value)

            try:
                value = decimal.Decimal(value)
            except Exception, exc:
                raise ValidationError('field cannot convert to decimal: {}'.format(exc))

        if self.min_value is not None and value < self.min_value:
            raise ValidationError('field value is too small')

        if self.max_value is not None and value > self.max_value:
            raise ValidationError('field value is too large')


class DateTimeField(BaseField):
    def __init__(self, **kwargs):
        self._meta = {
            'type': 'date'
        }
        super(DateTimeField, self).__init__(**kwargs)

    def to_python(self, value):
        if value is None:
            return value
        if isinstance(value, datetime.datetime):
            return value
        if isinstance(value, datetime.date):
            return datetime.datetime(value.year, value.month, value.day)
        if callable(value):
            return value()

        if not isinstance(value, basestring):
            return None

        # Attempt to parse a datetime:
        if dateutil:
            try:
                return dateutil.parser.parse(value)
            except ValueError:
                return None

        # split usecs, because they are not recognized by strptime.
        if '.' in value:
            try:
                value, usecs = value.split('.')
                usecs = int(usecs)
            except ValueError:
                return None
        else:
            usecs = 0
        kwargs = {'microsecond': usecs}
        try:  # Seconds are optional, so try converting seconds first.
            return datetime.datetime(*time.strptime(value,
                                     '%Y-%m-%d %H:%M:%S')[:6], **kwargs)
        except ValueError:
            try:  # Try without seconds.
                return datetime.datetime(*time.strptime(value,
                                         '%Y-%m-%d %H:%M')[:5], **kwargs)
            except ValueError:  # Try without hour/minutes/seconds.
                try:
                    return datetime.datetime(*time.strptime(value,
                                             '%Y-%m-%d')[:3], **kwargs)
                except ValueError:
                    return None

    def validate(self, value):
        new_value = self.to_python(value)
        if not isinstance(new_value, (datetime.datetime, datetime.date)):
            raise ValidationError('field must be a valid date/datetime object')