import pandas as pd
import numpy as np
import sqlalchemy
from collections import defaultdict
import functools
import re
import settings

df = pd.read_csv(settings.APA_PRIAME_ZIADOSTI,
                 sep=';',
                 engine='python',
                 decimal=',',
                 dtype={'ICO': str})

re_is_number = re.compile('^[0-9]*$')
def fix_repeated_names(threshold, s):
    if re_is_number.match(s):
        return s
    len_s=len(s)
    indexes_of_is_own_suffix = [ ii for ii in
                [i for i in range(len_s//2,len_s) if s[i] == s[0]]
                if s[ii:] == s[0:len(s[ii:])]
            ]
    if indexes_of_is_own_suffix:
        suffix_length = len_s - min(indexes_of_is_own_suffix)
        if suffix_length>=threshold:
            return s[:-suffix_length]
    return s


df['Ziadatel'] = df['Ziadatel'].apply(functools.partial(fix_repeated_names, 4))

conn = sqlalchemy.create_engine(settings.DATABASE_URL)
types_dict = {
	'URL': sqlalchemy.types.TEXT,
	'Ziadatel': sqlalchemy.types.TEXT,
	'ICO': sqlalchemy.types.TEXT,
	'Rok': sqlalchemy.types.INT,
	'Ziadosti': sqlalchemy.types.TEXT,
}

df.to_sql('apa_ziadosti_o_priame_podpory', con=conn, index=False, dtype=types_dict, if_exists='replace')
