from .query import Q
from .filter import F
from .aggs import A
from .function import SF
from .search import Search
from .fields import *
from .document import BaseDocument
from .mapping import Mapping

VERSION = (0, 0, 4, 'dev')
__version__ = VERSION
__versionstr__ = '.'.join(map(str, VERSION))
