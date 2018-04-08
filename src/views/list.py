from elasticsearch import Elasticsearch
from flask import request, jsonify
from flask_restful import Resource

from db.manager import get_conn
import settings

conn = get_conn()


def append_range_filter(f, key, _from, to):
    d = {}

    if _from or to:
        d['range'] = {}
        d['range'][key] = {}
    if _from:
        d['range'][key]['gte'] = _from
    if to:
        d['range'][key]['lte'] = to

    f.append(d)

    return f


class ListPO(Resource):

    def get(self):
        q = request.args.get('q', None)

        es = Elasticsearch(
            [settings.ELASTIC_HOST, ],
            timeout=30, max_retries=10, retry_on_timeout=True, port=settings.ELASTIC_PORT
        )

        if not q:
            query = {'query': {'match_all': {}}}
            results = es.search(index='apa', doc_type='po', body=query)
            rows = [{
                'data': r['_source'], '_id': r['_id']
            } for r in results['hits']['hits']]

            return jsonify(rows)

        rok_from = request.args.get('rok_from', None)
        rok_to = request.args.get('rok_to', None)
        suma_from = request.args.get('suma_from', None)
        suma_to = request.args.get('suma_to')

        # append filters
        f = []
        append_range_filter(f, 'rok', rok_from, rok_to)
        append_range_filter(f, 'suma', suma_from, suma_to)

        query = {
            "sort": [
                {"suma": {"order": "desc"}}
            ],
            "query": {
                "bool": {
                    "must": [
                        {
                            "match": {
                                "meno": {"query":q, "operator": "and"}
                            }
                        },
                    ],
                    # "filter": []
                }
            }
        }
        query['query']['bool']['must'].extend(f)

        results = es.search(index='apa', doc_type='po', body=query)

        rows = [{
            'data': r['_source'], '_id': r['_id']
        } for r in results['hits']['hits']]

        return rows
