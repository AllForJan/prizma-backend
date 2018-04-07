from flask import request, jsonify
from flask_restful import Resource

from db.manager import get_conn
from utils import load_sql


conn = get_conn()


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
