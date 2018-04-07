from elasticsearch import Elasticsearch
from flask import request, jsonify
from flask_restful import Resource

from db.manager import get_conn


conn = get_conn()


def append_range_filter(f, key, _from, to):
    if _from or to:
        f['range'] = {}
        f['range'][key] = {}
    if _from:
        f['range'][key]['gte'] = _from
    if to:
        f['range'][key]['lte'] = to
    return f


class ListPO(Resource):

    def get(self):
        q = request.args.get('q', 'Brezolupy')
        rok_from = request.args.get('rok_from')
        rok_to = request.args.get('rok_to')
        suma_from = request.args.get('suma_from')
        suma_to = request.args.get('suma_to')

        es = Elasticsearch(
            ['elasticsearch', ],
            timeout=30, max_retries=10, retry_on_timeout=True, port=9200
        )

        # append filters
        f = {}
        append_range_filter(f, 'rok', rok_from, rok_to)
        append_range_filter(f, 'suma', suma_from, suma_to)

        query = {
            "sort": [
                {"suma": {"order": "desc"}}
            ],
            "query": {
                "bool": {
                    "must": {
                        "match": {
                            "meno": q
                        }
                    },
                    "filter": f
                }
            }
        }
        results = es.search(index='apa', doc_type='po', body=query)

        rows = [{
            'data': r['_source'], '_id': r['_id']
        } for r in results['hits']['hits']]

        return rows
