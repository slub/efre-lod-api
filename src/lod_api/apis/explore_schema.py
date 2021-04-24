from copy import deepcopy
from schema import (
        Schema,
        Optional,
        Use
        )

### topicsearch ###
_additionalType = Schema({
    'name': str,
    'description': str,
    Optional('id'): str
    })

_topic = Schema({
    'id': str,
    'name': str,
    Optional('alternateName',   default=[]): [str],
    Optional('description',     default=""): str,
    Optional('additionalTypes', default=[]): [_additionalType],
    })

topicsearch_schema = deepcopy(_topic)
topicsearch_schema.schema['score'] = float

### aggregation schema ###
_topAuthor = Schema({
    'key': str,
    'docCount': int
    })

_datePublished = Schema({
    'year': int,
    'count': int
    })

_mention = Schema ({
    'key': str,
    'docCount': int
    })

_resourceAggregation = Schema({
    'docCount': int,
    'topAuthors': [_topAuthor],
    'datePublished': [_datePublished],
    'mentions': [_mention]
    })

aggregations_schema = _resourceAggregation

_resource = Schema(Use(dict))
_person = Schema(Use(dict))
_geo = Schema(Use(dict))
_event = Schema(Use(dict))

_entities = Schema({
    'resources': [_resource],
    'authors': [_person],
    'locations': [_geo],
    'events': [_event],
    'relatedTopics': [_topic]
    })

aggregation_su_schema = Schema({
    'strictAgg': _resourceAggregation,
    Optional('looseAgg'): _resourceAggregation,
    Optional('superAgg'): _resourceAggregation,
    Optional('entityPool'): _entities,
    })
