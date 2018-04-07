import pandas as pd
import sqlalchemy

import settings

df_ziadosti=pd.read_csv(settings.APA_ZIADOSTI_O_PROJEKTOVE_PODPORY,
                        sep=';',
                        engine='python',
                        decimal=',',
                        dtype={'ICO': str}
                        )
ziadosti_types = {
    'Ziadatel': sqlalchemy.types.TEXT,
    'ICO': sqlalchemy.types.TEXT,
    'Kod projektu': sqlalchemy.types.TEXT,
    'Nazov projektu': sqlalchemy.types.TEXT,
    'VUC': sqlalchemy.types.TEXT,
    'Cislo vyzvy': sqlalchemy.types.TEXT,
    'Kod podopatrenia': sqlalchemy.types.TEXT,
    'Status': sqlalchemy.types.TEXT,
    'Datum RoN/datum zastavenia konania': sqlalchemy.types.DATE,
    'Dovod RoN/zastavenie konania': sqlalchemy.types.TEXT,
    'Datum ucinnosti zmluvy': sqlalchemy.types.DATE,
    'Schvaleny NFP celkom': sqlalchemy.types.FLOAT,
    'Vyplateny NFP celkom': sqlalchemy.types.FLOAT,
    'Pocet bodov': sqlalchemy.types.INT,
}

for col_name in ['Datum RoN/datum zastavenia konania', 'Datum ucinnosti zmluvy']:
    df_ziadosti[col_name] = pd.to_datetime(
        df_ziadosti[col_name], errors='coerce', format='%d %m %Y'
    )

df_ziadosti.to_sql(
    'apa_ziadosti', con=conn, index=False, dtype=ziadosti_types, if_exists='replace'
)
