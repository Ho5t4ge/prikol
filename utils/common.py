#  Copyright (c) 2010-2024. LUKOIL-Engineering Limited KogalymNIPINeft Branch Office in Tyumen
#  Данным программным кодом владеет Филиал ООО "ЛУКОЙЛ-Инжиниринг" "КогалымНИПИнефть" в г.Тюмени

import re


def replace_letter(name):
    replacements = {'G': 'Г', 'T': 'Т', 'L': 'Л', 'P': 'П', 'N': 'Н', 'M': 'М', 'U': 'У', 'D': 'Д', 'B': 'Б', 'R': 'Р'}
    match = re.search(r'_(\d+[A-Z]?)', name)
    if match:
        well_number = match.group(1)
        for eng, rus in replacements.items():
            well_number = well_number.replace(eng, rus)
        return well_number
    return name


def get_field_id(name, field_ids):
    match = re.search(r'_(\w*?)_', name)
    if match:
        return field_ids.get(match.group(1), 0)
    else:
        return 0


def find_wellid(base_name, field_id, well_id_map):
    return well_id_map.get(f'{base_name}_{field_id}', 0)
