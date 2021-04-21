from schema import (
        Schema,
        Optional
        )

_additionalType = Schema({
    'name': str,
    'description': str,
    Optional('id'): str
    })

_topAuthor = Schema({
    'key': str,
    'doc_count': int
    })

_datePublished = Schema({
    'year': int,
    'count': int
    })

_mention = Schema ({
    'name': str,
    'docCount': int
    })

_resourceAggregation = Schema({
    'docCount': int,
    'topAuthors': [_topAuthor],
    'datePublished': [_datePublished],
    'mentions': [_mention]
    })

topicsearch = Schema({
    'id': str,
    'name': str,
    Optional('alternateName',   default=[]): [str],
    Optional('description',     default=""): str,
    Optional('score',           default=0):  float,
    Optional('additionalTypes', default=[]): [_additionalType],
    Optional('aggregations'): _resourceAggregation,
    })

