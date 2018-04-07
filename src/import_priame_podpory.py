import pandas as pd
import sqlalchemy

import settings

df = pd.read_csv(settings.APA_PRIAME_ZIADOSTI, sep=';', engine='python')
df = df.rename(columns={
    'URL': 'url',
    'Ziadatel': 'meno',
    'Ziadosti': 'ziadosti',
    'ICO': 'ico',
    'Rok': 'rok',
    'custom_id': 'custom_id',
})
types_dict = {
    'URL': sqlalchemy.types.TEXT,
    'Ziadatel': sqlalchemy.types.TEXT,
    'Rok': sqlalchemy.types.INT,
    'Ziadosti': sqlalchemy.types.TEXT,
    'custom_id': sqlalchemy.types.INT,
    'ICO': sqlalchemy.types.INT,
}

conn = sqlalchemy.create_engine(settings.DATABASE_URL)
df.to_sql('apa_priame_ziadosti', con=conn, index=False, dtype=types_dict, if_exists='replace')
