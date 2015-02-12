from elasticsearch_dsl.connections import connections
from elasticsearch.helpers import bulk
from retrying import retry


__author__ = 'Matthew Moon'
__filename__ = 'queue'

@retry(wait_fixed=60000)
def _send_to_es(instance, index):
    es = connections.get_connection(instance.using)
    bulk(client=es, index=index, actions=instance._iter_queue(index), chunk_size=instance.limit, timeout=60)
    instance._queue[index] = []

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
        _send_to_es(self, (index or self.index))


