import datetime

from elasticsearch_dsl.document import BaseDocument
from elasticsearch_dsl.fields import *
class MyDoc(BaseDocument):
    title = StringField(index='analyzed')
    name = StringField()
    created_at = DateTimeField()

class MySubDoc(MyDoc):
    name = StringField(index='not_analyzed')

    meta = {
        "doc_type": "node_stats",
        "index": ".marvel*"
    }


def test_declarative_mapping_definition():
    assert issubclass(MyDoc, BaseDocument)
    assert hasattr(MyDoc, 'meta')
    assert 'my_doc' == MyDoc.meta.name
    assert {
        'my_doc': {
            'properties': {
                'created_at': {'type': 'date'},
                'name': {'type': 'string'},
                'title': {'index': 'analyzed', 'type': 'string'},
            }
        }
    } == MyDoc.meta.mapping.to_dict()

# def test_you_can_supply_own_mapping_instance():
#     class MyD(document.DocType):
#         title = field.String()
#
#         class Meta:
#             mapping = Mapping('my_d')
#             mapping.meta('_all', enabled=False)
#
#     assert {
#         'my_d': {
#             '_all': {'enabled': False},
#             'properties': {'title': {'type': 'string'}}
#         }
#     } == MyD._doc_type.mapping.to_dict()
#
def test_document_can_be_created_dynamicaly():
    n = datetime.datetime.utcnow()
    md = MyDoc(title='hello')
    md.name = 'My Fancy Document!'
    md.created_at = n

    assert {
        'title': 'hello',
        'name': 'My Fancy Document!',
        'created_at': n,
    } == md.to_dict()
#
def test_document_inheritance():
    assert issubclass(MySubDoc, MyDoc)
    assert issubclass(MySubDoc, BaseDocument)
    assert hasattr(MySubDoc, 'meta')
    assert 'my_custom_doc' == MySubDoc.meta.name
    assert {
        'my_custom_doc': {
            'properties': {
                'created_at': {'type': 'date'},
                'name': {'type': 'string', 'index': 'not_analyzed'},
                'title': {'index': 'analyzed', 'type': 'string'},
            }
        }
    } == MySubDoc.meta.mapping.to_dict()
#
# def test_meta_fields_are_stored_in_meta_and_ignored_by_to_dict():
#     md = MySubDoc(id=42, name='My First doc!')
#
#     md._meta.index = 'my-index'
#     assert md._meta.index == 'my-index'
#     assert md.id == 42
#     assert {'name': 'My First doc!'} == md.to_dict()
#     assert {'id': 42, 'index': 'my-index'} == md._meta.to_dict()
#
# def test_meta_inheritance():
#     assert issubclass(MyMultiSubDoc, MySubDoc)
#     assert issubclass(MyMultiSubDoc, MyDoc2)
#     assert issubclass(MyMultiSubDoc, document.DocType)
#     assert hasattr(MyMultiSubDoc, '_doc_type')
#     # doc_type should not be inherited
#     assert 'my_multi_sub_doc' == MyMultiSubDoc._doc_type.name
#     # index and using should be
#     assert MyMultiSubDoc._doc_type.index == MySubDoc._doc_type.index
#     assert MyMultiSubDoc._doc_type.using == MySubDoc._doc_type.using
#     assert {
#         'my_multi_sub_doc': {
#             'properties': {
#                 'created_at': {'type': 'date'},
#                 'name': {'type': 'string', 'index': 'not_analyzed'},
#                 'title': {'index': 'not_analyzed', 'type': 'string'},
#                 'inner': {
#                     'type': 'object',
#                     'properties': {'old_field': {'type': 'string'}}
#                 },
#                 'extra': {'type': 'long'}
#             }
#         }
#     } == MyMultiSubDoc._doc_type.mapping.to_dict()


if __name__ == "__main__":
    test_declarative_mapping_definition()
    test_document_can_be_created_dynamicaly()
    print MySubDoc.query[:200].execute()
    # test_document_inheritance()