import subprocess


'''Utility functions to distribute configs.
Requires:
    rados_util'''

def send_config_with_keys(nodes, ceph_deploypath, silent):
    '''Pushes configuration and client.admin.key to given hosts.
    Args:
        nodes (iterable(str)): Iterable of hostnames to push config to.
        ceph_deploypath (str): Path to ceph-deploy binary.
        silent (bool): If set, prints less output.

    Returns:
        `True` on success, `False` otherwise.'''
    cmd = '{} --overwrite-conf admin {}'.format(ceph_deploypath, ' '.join(x.hostname for x in nodes))
    return subprocess.call(cmd, **get_subprocess_kwargs(silent)) == 0


def send_config(nodes, ceph_deploypath, silent):
    cmd = '{} --overwrite-conf config push {}'.format(ceph_deploypath, ' '.join(x.hostname for x in nodes))
    return subprocess.call(cmd, **get_subprocess_kwargs(silent)) == 0