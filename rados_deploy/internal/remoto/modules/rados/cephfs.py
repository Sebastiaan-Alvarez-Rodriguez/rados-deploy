import subprocess

import remoto.process


'''Utility functions to interact with CephFS.
Requires:
    config
    rados_util'''
def stop_cephfs(connection, path='/mnt/cephfs', silent=False):
    '''Stops cephfs on remote machine. Does not return anything.'''
    remoto.process.check(connection, 'sudo fusermount -uz {}'.format(path), shell=True)


def start_cephfs(node, connection, ceph_deploypath, path='/mnt/cephfs', use_client_cache=True, retries=5, silent=False):
    '''Starts cephFS on /mnt/cephfs.
    Warning: This function fails when cephfs is already mounted.
    Args:
        node (metareserve.Node): Node to start CephFS on.
        connection (remoto.Connection): Connection to use for deploying.
        ceph_deploypath (str): Path to `ceph-deploy` executable.
        path (optional str): Path to mount CephFS on.
        use_client_cache (optional bool): Toggles using CephFS I/O cache.
        retries (optional int): Number of tries we try to perform potentially-crashing operations.
        silent (optional bool): If set, does not print compilation progress, output, etc. Otherwise, all output will be printed.
        
    Returns:
        `True` on success, `False` on failure.'''
    remoto.process.check(connection, 'sudo mkdir -p {}'.format(path), shell=True)
    remoto.process.check(connection, 'sudo mkdir -p /etc/ceph'.format(path), shell=True)
    _, _, exitcode = remoto.process.check(connection, 'sudo apt update -y && sudo apt install ceph-fuse -y', shell=True)
    if exitcode != 0:
        return False

    if not send_config_with_keys([node], ceph_deploypath, silent):
        return False

    remoto.process.check(connection, 'sudo rm -rf {0}/* && sudo rm -rf {0}/.*'.format(path), shell=True)

    state_ok = False

    cmd = 'ceph-fuse'

    import time
    for x in range(retries):
        out, err, exitcode = remoto.process.check(connection, 'sudo {} {}'.format(cmd, path), shell=True)
        if exitcode == 0:
            prints('[{}] Succesfully called ceph-fuse (attempt {}/{}) (I/O caching={})'.format(node.hostname, x+1, retries, 'true' if use_client_cache else 'false'))    
            state_ok = True
            break
        else:
            printw('[{}] Executing ceph-fuse... (attempt {}/{})\n output: {}\n error: {}'.format(node.hostname, x+1, retries, out, err))
        time.sleep(1)
    if not state_ok:
        return False
    return remoto.process.check(connection, 'sudo chown -R {} {}'.format(node.extra_info['user'], path), shell=True)[2] == 0