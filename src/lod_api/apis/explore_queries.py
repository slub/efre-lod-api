_aggs = {
        "topAuthors": {
            "terms": {
                "field": 'author.@id.keyword',
                "size": 10
                }
            },
        "datePublished": {
            "date_histogram": {
                "field": "datePublished.@value",
                "fixed_interval": "1825d",
                "min_doc_count": 1,
                "format": "yyyy"
                }
            },
        "mentions": {
            "terms": {
                "field": "mentions.@id.keyword",
                "size": 10
                }
            },
        "genres": {
            "terms": {
                "field": 'genre.Text.keyword',
                "size": 20
                }
            },
        "topRelatedTopics": {
            "terms": {
                "field": 'mentions.@id.keyword',
                "include": '.*topics.*',
                "size": 10
                }
            }
        }

_sort = ["_score",
        {
            "datePublished.@value": {
                "order": "desc"
                }
            }
        ]

_fields = ['preferredName^2',
          'description',
          'alternativeHeadline',
          'nameShort',
          'nameSub',
          'author.name',
          'mentions.name^3',
          'partOfSeries.name',
          'about.name',
          'about.keywords'
          ]

def topic_aggs_query_strict(query):
    return {
        "size": 15,
        "sort": _sort,
        "query": {
            "bool": {
                "must" : [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": _fields,
                            "type": "phrase"
                            }
                        }
                    ],
                "filter": [
                    {
                        "term": {
                            'mentions.name.keyword': query
                            }
                        }
                    ]
                }
            },
        "aggs": _aggs
        }

def topic_aggs_query_loose(query):
    return {
        "size": 15,
        "sort": _sort,
        "query": {
            "multi_match": {
                "query": query,
                "fields": _fields,
                "type": "phrase"
                }
            },
        "aggs": _aggs
        }

def topic_query(query, size, fields, excludes):
    # TOOD: multi_match types best_fields, most_fields, cross_fields, phrase, phrase_prefix, bool_prefix
    # https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-multi-match-query.html
    # multi_match:
    #   query,
    #   fields: ['*'],
    #
    # type: 'most_fields'
    # }
    return {
        'size': size,
        '_source': excludes,
        'query': {
            "simple_query_string": {
                'query':  query,
                'fields': fields,
                'default_operator': 'and'
                }
            }
        }
