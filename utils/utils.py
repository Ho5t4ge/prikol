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
