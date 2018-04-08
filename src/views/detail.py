from flask import jsonify
from flask_restful import Resource

from db.manager import get_conn
from utils import load_sql

conn = get_conn()


class DetailPO(Resource):

    def get(self, custom_id):
        cur = conn.cursor()

        sql = load_sql('select_prijimatel.sql', kwargs={'custom_id': custom_id})
        cur.execute(sql)
        prijimatel_records = [row for row in cur]

        sql = load_sql('select_prijimatel_rok.sql', kwargs={'custom_id': custom_id})
        cur.execute(sql)
        prijimatel_roky = {'roky': [], 'sumy': []}
        for row in cur:
            prijimatel_roky['roky'].append(row['rok'])
            prijimatel_roky['sumy'].append(row['suma'])

        sql = load_sql('average_per_obec.sql', kwargs={'obec': obec})
        average_obec = cur.execute(sql)

        sql = load_sql('average_per_custom_id_and_obec.sql', kwargs={'obec': obec,
                                                                     'custom_id': custom_id})
        average_obec_custom_id = cur.execute(sql)

        cur.close()

        return jsonify(
            custom_id=custom_id,
            prijimatel_records=prijimatel_records,
            prijimatel_roky=prijimatel_roky,
            average_obec=average_obec,
            average_prijimal=average_obec_custom_id
        )
