import concurrent.futures
import tempfile
import uuid

from rados_deploy.thirdparty.sshconf import *

import logging
import remoto

from rados_deploy.internal.util.printer import *


class RemotoSSHWrapper(object):
    '''Simple wrapper containing a remoto connection and the file it is using as ssh config.'''
    def __init__(self, connection, ssh_config=None):
        self._connection = connection
        self._ssh_config = ssh_config
        self._open = True

    def __enter__(self):
        return self

    @property
    def connection(self):
        return self._connection
    
    @property
    def ssh_config(self):
        return self._ssh_config
    
    @property
    def ssh_config_path(self):
        return self._ssh_config.name

    @property
    def open(self):
        '''If set, connection is open. Otherwise, Connection is closed'''
        return self._open
    

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.exit()
        return False


    def exit(self):
        self._connection.exit()
        if self._ssh_config:
            self._ssh_config.close()
        self._open = False


def _build_ssh_config(hostname, ssh_params):
    '''Writes a temporary ssh config with provided parameters.
    Warning: Returned value must be closed properly.
    Args:
        hostname (str): Hostname to register.
        ssh_params (dict): Parameters to set for hostname. A valid dict would be e.g: {"IdentityFile": "/some/key.rsa", "IdentitiesOnly": "yes", "Port": 22}

    Returns:
        TemporaryFile containing the ssh config.'''
    if callable(ssh_params):
        ssh_params = ssh_params(node)
    if not isinstance(ssh_params, dict):
        raise ValueError('ssh_params must be a dict, mapping ssh options to values. E.g: {{"IdentityFile": "/some/key.rsa", "IdentitiesOnly": "yes", "Port": 22}}')
    conf = empty_ssh_config_file()
    conf.add(hostname, **ssh_params)
    tmpfile = tempfile.NamedTemporaryFile()
    conf.write(tmpfile.name)
    return tmpfile


def _build_conn(hostname, loggername, silent, ssh_configpath=None):
    '''Returns a remoto-wrapped execnet connection.
    Warning: The `remoto.Connection` objects created here must be properly closed.
    Args:
        hostname (str): Remote host (or ip) to connect to.
        silent (bool): If set, the connection is as silent as possible. Only stderr prints are logged (`logging.ERROR` level).
        loggername (str): If `silent` is set, sets quiet logger to have given name. This is useful for when you want to change the log level later.
        ssh_configpath (optional str): If set, sets execnet ssh config parameters to given path. This way, we can change ssh-based connection behaviour.

    Returns:
        configured `remoto.Connection` object on success, `None` on failure.'''
    logging.basicConfig()
    logger = logging.getLogger(loggername)
    logger.setLevel(logging.ERROR if silent else logging.DEBUG)

    kwargs = dict()
    kwargs['logger'] = logger
    if ssh_configpath:
        kwargs['ssh_options'] = '-F {}'.format(ssh_configpath)
    try:
        return remoto.Connection(hostname, **kwargs)
    except Exception as e:
        printe('Could not connect to remote host {}'.format(hostname))
        return None


def get_wrapper(node, hostname, ssh_params=None, loggername=None, silent=False):
    '''Gets a connection wrapper.
    Warning: The `RemotoSSHWrapper` objects created here must be properly closed. A "with" clause is supported to close all wrappers on function exit.
    Args:
        node (metareserve.Node): Node to build connection for.
        hostname (str, callable): Name to register connection to. Callables must take 1 node as argument, and output the hostname (`str`).
        ssh_params (optional dict, callable): If set, builds a temporary ssh config file with provided options to open connection with.
                                                       Can be a callable (i.e. function/lambda), which takes 1 node as argument, and outputs the dict with ssh config options (or `None`) for that node.
        loggername (optional str, callable): Name for logger. Can be either a `str` or a callable. Callables must take 1 node as argument, and output the logger name (`str`) to use for that node. If not set, uses random logger name.
        silent (optional bool): If set, connection is silent (except when reporting errors).

    Returns:
        `RemotoSSHWrapper` on success, `None` otherwise.'''
    if loggername == None:
        loggername = 'logger-'+str(uuid.uuid4())
    elif callable(loggername):
        loggername = loggername(node)

    if callable(hostname):
        hostname = hostname(node)

    if callable(ssh_params):
        ssh_params = ssh_params(node)

    ssh_config = _build_ssh_config(hostname, ssh_params) if ssh_params else None
    conn = _build_conn(hostname, loggername, silent, ssh_configpath=ssh_config.name if ssh_config else None)
    return RemotoSSHWrapper(conn, ssh_config=ssh_config)


def get_wrappers(nodes, hostnames, ssh_params=None, loggername=None, parallel=True, silent=False):
    '''Gets multiple wrappers at once.
    Warning: The `RemotoSSHWrapper` objects created here must be properly closed.
    Args:
        nodes (iterable of metareserve.Node): Nodes to build connection for.
        hostnames (dict(metareserve.Node, str), callable): Names to register connections to. Can be either a dict mapping nodes to their hostname or a callable taking 1 node as argument, outputting its hostname.
        ssh_params (optional dict or callable): If set, builds a temporary ssh config file with provided options to open connection with.
                                                       Can be a callable (i.e. function/lambda), which takes 1 node as argument, and outputs the dict with ssh config options (or `None`) for that node.
        loggername (optional callable): Callable must take 1 node as argument, and output the logger name (`str`) to use for that node. If not set, uses random logger names.
        parallel (optional bool): If set, creates wrappers in parallel. Otherwise, creates sequentially.
        silent (optional bool): If set, connections are silent (except when reporting errors).

    Returns:
        `dict(metareserve.Node, RemotoSSHWrapper)`, Maps metareserve.Node to open remoto connection wrapper. Wrapper can be `None`, indicating failure to connect to key node'''
    hostnames = hostnames if isinstance(hostnames, dict) else {x: hostnames(x) for x in nodes}
    if parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as executor:
            futures_get_wrappers = {x: executor.submit(get_wrapper, x, hostnames[x], ssh_params=ssh_params, loggername=loggername, silent=silent) for x in nodes}
            return {k: v.result() for k,v in futures_get_wrappers.items()}
    else:
        return {x: get_wrapper(x, hostnames[x], ssh_params=ssh_params, loggername=loggername, silent=silent) for x in nodes}


def close_wrappers(wrappers, parallel=True):
    '''Closes an iterable of wrappers.
    Args:
        wrappers (RemotoSSHWrapper, list(RemotoSSHWrapper), dict(RemotoSSHWrapper)): Wrappers to close.
        parallel (optional bool): If set, closes connections in parallel. Otherwise, closes connections sequentially.'''
    if isinstance(wrappers, RemotoSSHWrapper):
        closables = [wrappers]
    elif isinstance(wrappers, dict):
        if isinstance(list(wrappers.keys())[0], RemotoSSHWrapper):
            closables = wrappers.keys()
        elif isinstance(wrappers[list(wrappers.keys())[0]], RemotoSSHWrapper):
            closables = wrappers.values()
        else:
            raise ValueError('Provided dict has no RemotoSSHWrapper keys(={}) or values(={})'.format(type(list(wrappers.keys())[0]), type(wrappers[list(wrappers.keys())[0]])))
    elif isinstance(wrappers, list):
        closables = wrappers
    else:
        raise ValueError('Cannot close given wrappers: No dict, list, or single wrapper passed: {}'.format(wrappers))
    if parallel:
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(closables)) as executor:
            futures_close = [executor.submit(x.exit) for x in closables]
            for x in futures_close:
                x.result()
    else:
        for x in closables:
            x.exit()