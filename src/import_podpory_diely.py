import pandas as pd
import sqlalchemy

import settings

df = pd.read_csv(settings.APA_ZIADOSTI_O_DIELY, sep=';', engine='python')

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
types_dict = {
    'URL': sqlalchemy.types.TEXT,
    'Ziadatel': sqlalchemy.types.TEXT,
    'Diel': sqlalchemy.types.TEXT,
    'Vymera': sqlalchemy.types.TEXT,
    'Kultura': sqlalchemy.types.TEXT,
    'Lokalita': sqlalchemy.types.TEXT,
    'Rok': sqlalchemy.types.INT,
    'custom_id': sqlalchemy.types.INT,
    'ICO': sqlalchemy.types.INT,
}

conn = sqlalchemy.create_engine(settings.DATABASE_URL)
df.to_sql('apa_ziadosti_diely', con=conn, index=False, dtype=types_dict, if_exists='replace')
