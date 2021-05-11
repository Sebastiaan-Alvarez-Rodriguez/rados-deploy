from metareserve import Reservation as _Reservation
from rados_deploy.internal.util.printer import *

def read_reservation_cli():
    '''Read `MetaReserve.` from user input.'''
    print('Paste Reservation string here. Use <enter> twice to finish.')
    lines = []
    while True:
        line = input('')
        if not any(line):
            break
        lines.append(line)
    try:
        return _Reservation.from_string('\n'.join(lines))
    except Exception as e:
        printe('Could not read data from input. Was input malformed? ', e)
        return None

