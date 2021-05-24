import re

# based on https://stackoverflow.com/a/42865957/2002471
units = {'B': 1, 'KiB': 2**10, 'MiB': 2**20, 'GiB': 2**30, 'TiB': 2**40}

def to_bytes(string):
    size = size.upper()
    if not re.match(r' ', size):
        size = re.sub(r'([KMGT]?B)', r' \1', size)
    number, unit = [string.strip() for string in size.split()]
    return int(float(number)*units[unit])
