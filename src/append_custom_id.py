import random
import csv
custom_ids = {}
cisla = random.sample(range(1, 10000002), 10000000)


def get_dict(path_file_1):
    rows = []
    with open(path_file_1, encoding='utf-8') as input_file:
        reader = csv.DictReader(input_file, delimiter=';')

        i = iter(reader)
        while True:
            try:
                row = next(i)
                rows.append(row)
            except StopIteration:
                break
            except:
                pass

    for row in rows:
        name = row['Ziadatel']
        ico = row['ICO']
        if ico != '':
            custom_ids[name] = ico
        else:
            custom_ids[name] = cisla.pop(len(cisla) - 1)
    return custom_ids


def csv_append():
    path1 = '/data/apa_ziadosti_diely.csv'
    path2 = '/data/apa_ziadosti_o_priame_podpory_diely_custom_ids.csv'

    path3 = '/data/apa_ziadosti.csv'
    path4 = '/data/apa_ziadosti_o_priame_podpory_custom_ids.csv'

    path5 = '/data/apa_prijimatelia.csv'
    path6 = '/data/apa_prijimatelia_custom_ids.csv'

    custom_ids = get_dict('/data/apa_ziadosti_diely.csv')

    rows = []
    with open(path1, encoding='utf-8') as input_file:
        reader = csv.DictReader(input_file, delimiter=';')

        i = iter(reader)
        while True:
            try:
                row = next(i)
                rows.append(row)
            except StopIteration:
                break
            except:
                pass

    with open(path2, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(rows[0].keys()) + ['custom_id'], delimiter=';')
        writer.writeheader()
        for row in rows:
            try:
                row['custom_id'] = custom_ids[row['Ziadatel']]
                writer.writerow(row)
            except KeyError:
                pass

    rows = []
    with open(path3, encoding='utf-8') as input_file:
        reader = csv.DictReader(input_file, delimiter=';')

        i = iter(reader)
        while True:
            try:
                row = next(i)
                rows.append(row)
            except StopIteration:
                break
            except:
                pass

    with open(path4, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(rows[0].keys()) + ['custom_id'],
                                delimiter=';')
        writer.writeheader()
        for row in rows:
            try:
                row['custom_id'] = custom_ids[row['Meno']]
                writer.writerow(row)
            except KeyError:
                pass

    rows = []
    with open(path5, encoding='utf-8') as input_file:
        reader = csv.DictReader(input_file, delimiter=';')

        i = iter(reader)
        while True:
            try:
                row = next(i)
                rows.append(row)
            except StopIteration:
                break
            except:
                pass

    with open(path6, 'w') as output_file:
        writer = csv.DictWriter(output_file, fieldnames=list(rows[0].keys()) + ['custom_id'],
                                delimiter=';')
        writer.writeheader()
        for row in rows:
            try:
                row['custom_id'] = custom_ids[row['Ziadatel']]
                writer.writerow(row)
            except KeyError:
                pass

csv_append()
