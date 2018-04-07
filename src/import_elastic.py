import json
import re

import argparse
from elasticsearch import Elasticsearch
from sqlalchemy import create_engine
import pandas as pd

import settings

INDEX_NAME = "apa"
TYPE_NAME = "po"
LIMIT = 1000
STOP_WORDS = "data/stop.words.asciifold"


def cleanhtml(raw_html):
  cleanr = re.compile("<.*?>")
  cleantext = re.sub(cleanr, "", raw_html)
  return cleantext


def get_data():
    url = settings.DATABASE_URL
    engine = create_engine(url)
    df = pd.read_sql_table("apa_prijimatelia", engine)

    result_data = []
    columns = ['meno', 'psc', 'obec']
    # df['created'] = df['created'].astype(int)

    for i, d in enumerate(df[columns].to_dict("records")):
        op_dict = {
            "index": {
                "_index": INDEX_NAME,
                "_type": TYPE_NAME,
            }
        }

        result_data.append(op_dict)
        result_data.append(d)
        if i == LIMIT:
            break

    return result_data


def get_stop_words():
    with open(STOP_WORDS, "r") as f:
        sw = f.read().split(",")
        sw = [word.strip() for word in sw]
    return sw[:-1]


def get_mappings():
    return {
        "settings":{
            'number_of_shards': 1,

        },
            "mappings": {
                TYPE_NAME: {
                    "properties": {
                        "meno":{
                            "type": "text",
                        },
                        "psc": {
                            "type": "integer"
                        },
                        "obec":{
                            "type":"text"
                        }
                    }
                }
        }
    }


def refresh_all(es):
    data = get_data()

    if es.indices.exists(INDEX_NAME):
        print("Deleting index %s " % INDEX_NAME)
        es.indices.delete(INDEX_NAME)

    print("Creating index {i_name} with type {t_name}..."
          .format(i_name=INDEX_NAME, t_name=TYPE_NAME))
    mappings = get_mappings()
    res = es.indices.create(index=INDEX_NAME, body=mappings)

    print("Inserting all data - {len}".format(len=len(data)/2))

    # data are doubled, because there is dict about index and type for every row in data
    # so we perform bulk for 20000k - 10000k items
    j = 20000
    for i in range(0, len(data), 20000):
        res = es.bulk(index=INDEX_NAME, body=data[i:j], refresh=True)
        print("Bulk {i}-{j} inserted.".format(i=i, j=j))
        j += 20000


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--all",
                       default=False,
                       action="store_true",
                       help="Delete index, recreate mapping in elasticsearch and load data there."
                       )
    parser.add_argument("--limit",
                       type=int,
                       help="Limit for data"
                       )
    args = parser.parse_args()
    es = Elasticsearch(['elasticsearch',],
        timeout=30, max_retries=10, retry_on_timeout=True, port=9200
    )

    if args.limit:
        LIMIT = args.limit
    else:
        LIMIT = 100000000000

    if args.all:
        refresh_all(es)
    else:
        print("No inputs. Use --all to recreate index.")
