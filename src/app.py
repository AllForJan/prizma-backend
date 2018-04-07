import json
import itertools

from flask import Flask, render_template, jsonify, request
from flask_cors import CORS
from flask_restful import Resource, Api
import psycopg2
import psycopg2.extras
import settings
from utils import load_sql

app = Flask(__name__)
api = Api(app)
cors = CORS(app)
conn = psycopg2.connect(settings.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


@app.route('/')
def index():
    return 'hello'


class ListPO(Resource):

    def get(self):
        q = request.args.get('q', 'Brezolupy')

        cur = conn.cursor()
        sql = load_sql('example.sql', kwargs={'q': q})
        cur.execute(sql)
        rows = [row for row in cur]
        cur.close()

        return jsonify(
            po=rows
        )


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


api.add_resource(ListPO, '/po/list')
api.add_resource(GroupPO, '/po/group')

if __name__ == '__main__':
    app.run()
