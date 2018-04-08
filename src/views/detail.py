from collections import defaultdict

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

        sql = load_sql('select_ziadosti.sql', kwargs={'custom_id': custom_id})
        cur.execute(sql)
        prijimatel_ziadosti = [row for row in cur]

        sql = load_sql('select_prijimatel_rok.sql', kwargs={'custom_id': custom_id})
        cur.execute(sql)
        prijimatel_roky = {'roky': [], 'sumy': []}
        for row in cur:
            prijimatel_roky['roky'].append(row['rok'])
            prijimatel_roky['sumy'].append(row['suma'])

        url_diely = 'http://ppa.tools.bratia.sk/?parts='
        url_map = defaultdict(list)
        for ziadost in prijimatel_ziadosti:
            url_map[ziadost['lokalita']].append(ziadost['diel'])

        for lokalita, diel in url_map.items():
            url_diely += lokalita + ':' + '|'.join(diel) + ','

        url_diely = url_diely[:-1]

        #
        sql = load_sql('stats_diely.sql', kwargs={'custom_id': custom_id})
        cur.execute(sql)
        ziadosti_stats = {'roky': [], 'pocet': []}
        for row in cur:
            ziadosti_stats['roky'].append(row['rok'])
            ziadosti_stats['pocet'].append(row['pocet'])

        cur.close()

        return jsonify(
            custom_id=custom_id,
            prijimatel_records=prijimatel_records,
            prijimatel_ziadosti=prijimatel_ziadosti,
            prijimatel_roky=prijimatel_roky,
            url_diely=url_diely,
            ziadosti_stats=ziadosti_stats
        )
