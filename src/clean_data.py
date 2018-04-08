import pandas as pd
import re
import functools


def clean(input_file, output_file, key):
    df = pd.read_csv(input_file, sep=';', engine='python')

    re_is_number = re.compile('^[0-9]*$')

    def fix_repeated_names(s):
        try:
            if not s or re_is_number.match(s):
                return s
        except:
            pass
        try:
            len_s = len(s)
        except:
            return s

        indexes_of_is_own_suffix = [ii for ii in
                                    [i for i in range(len_s // 2, len_s) if s[i] == s[0]]
                                    if s[ii:] == s[0:len(s[ii:])]
                                    ]
        if indexes_of_is_own_suffix:
            suffix_length = len_s - min(indexes_of_is_own_suffix)
            if suffix_length >= 4:
                return s[:-suffix_length]
        return s

    df[key] = df[key].apply(fix_repeated_names)
    df.to_csv(output_file, index=False, sep=';')
