from schema import (
        Schema,
        Optional
        )

_additionalType = Schema({
    'name': str,
    'description': str,
    Optional('id'): str
    })

topicsearch = Schema({
    'id': str,
    'name': str,
    Optional('alternateName',   default=[]): [str],
    Optional('description',     default=""): str,
    Optional('score',           default=0):  float,
    Optional('additionalTypes', default=[]): [_additionalType]
    })

