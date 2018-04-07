import json
import itertools

from flask import Flask, render_template, jsonify, request
from flask_restful import Resource, Api
import psycopg2
import psycopg2.extras
import settings

app = Flask(__name__)
api = Api(app)
conn = psycopg2.connect(settings.DATABASE_URL, cursor_factory=psycopg2.extras.RealDictCursor)


@app.route('/')
def index():
    return 'hello'


class ListPO(Resource):

    def get(self):
        q = request.args.get('q', 'Brezolupy')

        cur = conn.cursor()

        cur.execute("select * from apa_prijimatelia where obec='{q}'".format(q=q))
        rows = [row for row in cur]
        cur.close()

        return jsonify(
            po=rows
        )


api.add_resource(ListPO, '/po/list')

if __name__ == '__main__':
    app.run()
