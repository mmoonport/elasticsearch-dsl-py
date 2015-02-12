from elasticsearch_dsl.connections import connections
from elasticsearch_dsl.utils import _bulk


__author__ = 'Matthew Moon'
__filename__ = 'queue'



class Queue(object):
    def __init__(self, index=None, using=None, limit=None):
        self.index = index
        self.using = using
        self.limit = limit or 100
        self._queue = {}

    def append(self, document, index='default'):
        if index not in self._queue.keys():
            self._queue[index] = []
        self._queue[index].append(document)

        if len(self._queue[index]) >= self.limit:
            self._send(index)

    def _iter_queue(self, queue='default'):
        for item in self._queue[queue]:
            yield item.to_es()

    def _send(self, index):
        es = connections.get_connection(self.using)
        index = (index or self.index)
        _bulk(conn=es, index=index, actions=self._iter_queue(index), chunk_size=self.limit, timeout=60)
        self._queue[index] = []


