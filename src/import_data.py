import pandas as pd
import numpy as np
import sqlalchemy
from elasticsearch import Elasticsearch
from collections import defaultdict
import functools
import re
import settings

import import_elastic


global_log_fixes = []


def fix_repeated_names(threshold, s, log_fixes=None):
    """
    Function will trim longest suffix holding property, that it is prefix too:

    Maximum lenght trimmed is half of string.

    Abcd12345Abcd1 -> Abcd12345

    :param threshold: suffix has to be at least as long to be trimmed
    :type threshold: int
    :param s: string to trim
    :type s: str
    :param log_fixes: if present, log changes here by appending (original, fixed, trimmed length)
    :type log_fixes: list
    :return: trimmed string
    :rtype: str
    """
    # ICO short-circuit
    if isinstance(s, float):
        return str(s)
    if s.isdigit():
        return s
    len_s = len(s)
    indexes_of_is_own_suffix = [
        ii for ii in [
            i for i in range(len_s // 2, len_s) if s[i] == s[0]
        ] if s[ii:] == s[0:len(s[ii:])]
    ]
    if indexes_of_is_own_suffix:
        suffix_length = len_s - min(indexes_of_is_own_suffix)
        if suffix_length >= threshold:
            fixed_s = s[:-suffix_length]
            if log_fixes:
                log_fixes.append((s, fixed_s, suffix_length))
            return fixed_s
    return s


def get_apa_prijimatelia(csv_file_path):
    df = pd.read_csv(
        csv_file_path,
        sep=';',
        engine='python',
        # dtype={
        #     'URL': str,
        #     'Meno': str,
        #     'PSC': str,
        #     'Obec': str,
        #     'Opatrenie': str,
        #     'Opatrenie - Kod': str,
        #     'Suma': float,
        #     'Rok': int,
        # }
    )
    df['PSC'] = df['PSC'].apply(lambda x: x.replace(' ', ''))
    df['Meno'] = df['Meno'].apply(functools.partial(fix_repeated_names, 4, log_fixes=global_log_fixes))

    df = df.rename(columns={
        'URL': 'url',
        'Meno': 'meno',
        'PSC': 'psc',
        'Obec': 'obec',
        'Opatrenie - Kod': 'opatrenie_kod',
        'Opatrenie': 'opatrenie',
        'Suma': 'suma',
        'Rok': 'rok',
        'custom_id': 'custom_id'
    })
    types_dict = {
        'url': sqlalchemy.types.TEXT,
        'meno': sqlalchemy.types.TEXT,
        'psc': sqlalchemy.types.TEXT,
        'obec': sqlalchemy.types.TEXT,
        'opatrenie': sqlalchemy.types.TEXT,
        'opatrenie_kod': sqlalchemy.types.TEXT,
        'suma': sqlalchemy.types.FLOAT,
        'rok': sqlalchemy.types.INT,
        'custom_id': sqlalchemy.types.INT,
    }
    return df, types_dict


def get_apa_ziadosti_o_priame_podpory_diely(csv_file_path):
    df = pd.read_csv(
        csv_file_path,
        sep=';',
        engine='python',
        dtype={
            'URL': str,
            'Ziadatel': str,
            'ICO': str,
            # 'Rok': int,
            'Lokalita': str,
            'Diel': str,
            'Kultura': str,
            'Vymera': str,
        }
    )

    df['Ziadatel'] = df['Ziadatel'].apply(functools.partial(fix_repeated_names, 4, log_fixes=global_log_fixes))
    df = df.rename(columns={
        'URL': 'url',
        'Ziadatel': 'meno',
        'Diel': 'diel',
        'Vymera': 'vymera',
        'Kultura': 'kultura',
        'Lokalita': 'lokalita',
        'Rok': 'rok',
        'custom_id': 'custom_id',
        'ICO': 'ico'
    })
    df['vymera'] = df['vymera'].apply(lambda x: float(x[:-3]) if not isinstance(x, float) and x.endswith(' ha') else x)
    types_dict = {
        'url': sqlalchemy.types.TEXT,
        'meno': sqlalchemy.types.TEXT,
        'ico': sqlalchemy.types.TEXT,
        'rok': sqlalchemy.types.INT,
        'lokalita': sqlalchemy.types.TEXT,
        'diel': sqlalchemy.types.TEXT,
        'kultura': sqlalchemy.types.TEXT,
        'vymera': sqlalchemy.types.FLOAT,
        'custom_id': sqlalchemy.types.INT,
    }
    return df, types_dict


def get_apa_ziadosti_o_projektove_podpory(csv_file_path):
    df = pd.read_csv(
        csv_file_path,
        sep=';',
        engine='python',
        decimal=',',
        # dtype={
        #   'Ziadatel': str,
        #   'ICO': str,
        #   'Kod projektu': str,
        #   'Nazov projektu': str,
        #   'VUC': str,
        #   'Cislo vyzvy': str,
        #   'Kod podopatrenia': str,
        #   'Status': str,
        #   'Datum RoN/datum zastavenia konania': str,
        #   'Dovod RoN/zastavenie konania': str,
        #   'Datum ucinnosti zmluvy': str,
        #   'Schvaleny NFP celkom': float,
        #   'Vyplateny NFP celkom': float,
        #   'Pocet bodov': int,
        # }
    )

    df['Ziadatel'] = df['Ziadatel'].apply(functools.partial(fix_repeated_names, 4, log_fixes=global_log_fixes))
    df = df.rename(columns={
        'Ziadatel': 'meno',
        'ICO': 'ico',
        'Kod projektu': 'kod_projektu',
        'Nazov projektu': 'nazov_projektu',
        'VUC': 'vuc',
        'Cislo vyzvy': 'cislo_vyzvy',
        'Kod podopatrenia': 'kod_podopatrenia',
        'Status': 'status',
        'Datum RoN/datum zastavenia konania': 'datum_ron_zastavenia_konania',
        'Dovod RoN/zastavenie konania': 'dovod_ron_zastavenie_konania',
        'Datum ucinnosti zmluvy': 'datum_ucinnosti_zmluvy',
        'Schvaleny NFP celkom': 'schvaleny_nfp_celkom',
        'Vyplateny NFP celkom': 'vyplateny_nfp_celkom',
        'Pocet bodov': 'pocet_bodov',
        'custom_id': 'custom_id',
    })

    for col_name in ['datum_ron_zastavenia_konania', 'datum_ucinnosti_zmluvy']:
        df[col_name] = pd.to_datetime(
            df[col_name], errors='coerce', format='%d %m %Y'
        )

    types_dict = {
        'meno': sqlalchemy.types.TEXT,
        'ico': sqlalchemy.types.TEXT,
        'kod_projektu': sqlalchemy.types.TEXT,
        'nazov_projektu': sqlalchemy.types.TEXT,
        'vuc': sqlalchemy.types.TEXT,
        'cislo_vyzvy': sqlalchemy.types.TEXT,
        'kod_podopatrenia': sqlalchemy.types.TEXT,
        'status': sqlalchemy.types.TEXT,
        'datum_ron_zastavenia_konania': sqlalchemy.types.DATE,
        'dovod_ron_zastavenie_konania': sqlalchemy.types.TEXT,
        'datum_ucinnosti_zmluvy': sqlalchemy.types.DATE,
        'schvaleny_nfp_celkom': sqlalchemy.types.FLOAT,
        'vyplateny_nfp_celkom': sqlalchemy.types.FLOAT,
        'pocet_bodov': sqlalchemy.types.INT,
        'custom_id': sqlalchemy.types.INT,
    }

    return df, types_dict


def get_apa_ziadosti_o_priame_podpory(csv_file_path):
    df = pd.read_csv(
        csv_file_path,
        sep=';',
        engine='python',
        decimal=',',
        # dtype={
        #     'URL': str,
        #     'Ziadatel': str,
        #     'ICO': str,
        #     'Rok': int,
        #     'Ziadosti': str,
        # }
    )

    df['Ziadatel'] = df['Ziadatel'].apply(functools.partial(fix_repeated_names, 4, log_fixes=global_log_fixes))

    df = df.rename(columns={
        'URL': 'url',
        'Ziadatel': 'meno',
        'ICO': 'ico',
        'Rok': 'rok',
        'Ziadosti': 'ziadosti',
        'custom_id': 'custom_id',
    })

    types_dict = {
        'url': sqlalchemy.types.TEXT,
        'meno': sqlalchemy.types.TEXT,
        'ico': sqlalchemy.types.TEXT,
        'rok': sqlalchemy.types.INT,
        'ziadosti': sqlalchemy.types.TEXT,
        'custom_id': sqlalchemy.types.INT,
    }
    return df, types_dict


curr_id = 0


def get_next_id():
    global curr_id
    curr_id += 1
    return curr_id


def import_csvs():
    db_conn = sqlalchemy.create_engine(settings.DATABASE_URL)

    ids_map = {}

    for get_fun, csv_path, table_name in [
        (
            get_apa_prijimatelia,
            settings.APA_PRIJIMATELIA,
            'apa_prijimatelia',
        ),
        (
            get_apa_ziadosti_o_priame_podpory_diely,
            settings.APA_ZIADOSTI_O_PRIAME_PODPORY_DIELY,
            'apa_ziadosti_diely',
        ),
        (
            get_apa_ziadosti_o_projektove_podpory,
            settings.APA_ZIADOSTI_O_PROJEKTOVE_PODPORY,
            'apa_ziadosti_o_projektove_podpory',
        ),
        (
            get_apa_ziadosti_o_priame_podpory,
            settings.APA_ZIADOSTI_O_PRIAME_PODPORY,
            'apa_ziadosti_o_priame_podpory',
        ),
    ]:
        print('Parsing data for {} from "{}"'.format(table_name, csv_path))
        try:
            df, types_dict = get_fun(csv_path)
            df = df.sort_values('meno', ascending=False)[:10000]

            def gen_id(x):
                global curr_id
                from_map = ids_map.get(x, None)
                new_id = from_map if from_map else get_next_id()
                ids_map[x] = new_id
                return new_id

            try:
                df = df.drop('custom_id')
            except:
                pass

            df['custom_id'] = df['meno'].apply(gen_id)

        except Exception as e:
            print('ERROR: "{}" while parsing {}'.format(e, csv_path))
            continue

        print('Importing table to DB. {sh}'.format(sh=df.shape))
        try:
            df.to_sql(table_name, con=db_conn, index=False, dtype=types_dict, if_exists='replace')
        except Exception as e:
            print('ERROR: "{}" while importing table {}'.format(e, table_name))
            continue
        print('Table {} imported'.format(table_name))

    print("Importing to elasticsearch ....")
    # es = Elasticsearch([settings.ELASTIC_HOST, ],
    #                    timeout=30, max_retries=10, retry_on_timeout=True, port=settings.ELASTIC_PORT
    #                    )
    # import_elastic.refresh_all(es)
    print("Imported  OK")


if __name__ == '__main__':
    import_csvs()
    print("\nfixes:")
    from pprint import pprint
    pprint(global_log_fixes)
