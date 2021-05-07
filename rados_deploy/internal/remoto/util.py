import logging
import remoto
import tempfile
from internal.remoto.ssh_wrapper import RemotoSSHWrapper
from thirdparty.sshconf import *
from internal.util.printer import *

def _get_logger(loggername, loglevel):
    # logging.basicConfig()
    # logger = logging.getLogger(loggername)
    # logger.setLevel(loglevel)
    logger = logging.getLogger(loggername)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(loglevel)
    return logger

def debug_logger(loggername):
    return _get_logger(loggername, logging.DEBUG)


def quiet_logger(loggername):
    return _get_logger(loggername, logging.ERROR)


def _connection(remote_hostname, silent, loggername, ssh_configpath=None):
    '''Returns a remoto-wrapped execnet connection.
    Args:
        remote_hostname (str): Remote host (or ip) to connect to.
        silent (bool): If set, the connection is as silent as possible. Only stderr prints are logged (`logging.ERROR` level).
        loggername (str): If `silent` is set, sets quiet logger to have given name. This is useful for when you want to change the log level later.
        ssh_configpath (optional str): If set, sets execnet ssh config parameters to given path. This way, we can change ssh-based connection behaviour.

    Returns:
        configured `remoto.Connection` object on success, `None` on failure.'''
    kwargs = dict()
    kwargs['logger'] = quiet_logger(loggername) if silent else debug_logger(loggername)
    if ssh_configpath:
        kwargs['ssh_options'] = '-F {}'.format(ssh_configpath)
    try:
        return remoto.Connection(remote_hostname, **kwargs)
    except Exception as e:
        printe('Could not connect to remote host {}'.format(remote_hostname))
        return None

def get_ssh_connection(remote_hostname, silent=True, loggername='SilentLogger', ssh_params=None):
    '''Returns a deploy-spark-wrapped execnet connection.
    Args:
        remote_hostname (str): Remote host (or ip) to connect to.
        silent (optional bool): If set, the connection only forwards stderr prints are logged (`logging.ERROR` level). Otherwise, prints all stdout/stderr output (`DEBUG`/`ERROR` levels, respectively).
        loggername (optional str): If `silent` is set, sets quiet logger to have given name. This is useful for when you want to change the log level later.
        tmpconf (optional `TmpSSHConfig`): If set, sets execnet ssh config parameters to given path. This way, we can change ssh-based connection behaviour.

    Returns:
        configured `RemotoSSHWrapper` object.'''
    if ssh_params:
        if not isinstance(ssh_params, dict):
            raise ValueError('If set, ssh_params must be a dictionary mapping SSH options to values. E.g: {{"IdentityFile": "/some/key.rsa", "IdentitiesOnly": "yes", "Port": 22}}')
        conf = empty_ssh_config_file() # function in thirdparty.sshconf
        conf.add(remote_hostname, **ssh_params)
        tmpfile = tempfile.NamedTemporaryFile()
        conf.write(tmpfile.name)
        return RemotoSSHWrapper(_connection(remote_hostname, silent, loggername, ssh_configpath=tmpfile.name), ssh_config=tmpfile)
    return RemotoSSHWrapper(_connection(remote_hostname, silent, loggername))