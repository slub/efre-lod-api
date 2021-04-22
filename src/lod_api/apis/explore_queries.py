aggs = {
        "topAuthors": {
            "terms": {
                "field": 'author.@id.keyword',
                "size": 10
                }
            },
        "datePublished": {
            "date_histogram": {
                "field": "datePublished.@value",
                "calendar_interval": "year",
                "min_doc_count": 1
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
            }
        }

def topic_aggs_query_strict(query, fields):
    return {
        "size": 15,
        "sort": [
            "_score",
            {
                "datePublished.@value": {
                    "order": "desc"
                    }
                }
            ],
        "query": {
            "bool": {
                "must" : [
                    {
                        "multi_match": {
                            "query": query,
                            "fields": fields,
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
        "aggs": aggs
        }
