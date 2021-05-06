import builtins
import socket
import sys

def print(*args, **kwargs):
    '''Print method overriding default, prints to stderr.'''
    kwargs['flush'] = True
    kwargs['file'] = sys.stderr
    
    return builtins.print('[{}] {}'.format(socket.gethostname(), args[0]), *(args[1:]), **kwargs) if any(args) else builtins.print(**kwargs)