class ReadOnlyException(Exception):
    pass

class ElasticsearchDslException(Exception):
    pass


class UnknownDslObject(ElasticsearchDslException):
    pass


class ValidationError(Exception):
    pass