#!/usr/bin/env bash
export DATABASE_URL="postgres://allfor:jan@localhost:5432/gis"
export ELASTIC_HOST="localhost"
export ELASTIC_PORT=9200

export APA_PRIJIMATELIA="$(pwd)/data2/apa_prijimatelia_2018-03-15.csv"
export APA_ZIADOSTI_O_PROJEKTOVE_PODPORY="$(pwd)/data2/apa_ziadosti-o-projektove-podpory_2018-04-03.csv"
export APA_ZIADOSTI_O_PRIAME_PODPORY="$(pwd)/data2/apa_ziadosti_o_priame_podpory_2018-03-20.csv"
export APA_ZIADOSTI_O_PRIAME_PODPORY_DIELY="$(pwd)/data2/apa_ziadosti_o_priame_podpory_diely_sample_2018-03-20.csv"
