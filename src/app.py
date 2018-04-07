from elasticsearch import Elasticsearch
from flask import Flask, jsonify, request
from flask_cors import CORS
from flask_restful import Resource, Api
from db.manager import get_conn
from utils import load_sql
from views.list import ListPO
from views.detail import DetailPO

app = Flask(__name__)
api = Api(app)
cors = CORS(app)
conn = get_conn()


@app.route('/')
def index():
    return 'hello'


class GroupPO(Resource):

    def get(self):
        q = request.args.get('q', 'Brezolupy')

        cur = conn.cursor()
        sql = load_sql('group_by.sql', kwargs={'q': q})
        cur.execute(sql)
        rows = [row for row in cur]
        cur.close()

        return jsonify(
            rows
        )


class AutoComplete(Resource):

    def get(self):
        q = request.args.get('q', 'Brezolupy')
        typ = request.args.get('typ', 'meno')

        if typ == 'meno':
            typ += '_autocomplete'
        print(typ)

        es = Elasticsearch(['elasticsearch', ],
                           timeout=30, max_retries=10, retry_on_timeout=True, port=9200
                           )

        query = {
            "query": {
                "match": {
                        typ: q
                    }
                }
            }
        results = es.search(index='apa', doc_type='po', body=query)

        rows = [{
                'data': r['_source'], '_id': r['_id']
        } for r in results['hits']['hits']]

        return jsonify(
            rows
        )


api.add_resource(AutoComplete, '/autocomplete')
api.add_resource(ListPO, '/po/list')
api.add_resource(GroupPO, '/po/group')
api.add_resource(DetailPO, '/po/<int:custom_id>')

if __name__ == '__main__':
    app.run()
