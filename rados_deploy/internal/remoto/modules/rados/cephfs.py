import subprocess

import remoto.process


'''Utility functions to interact with CephFS.
Requires:
    config
    rados_util'''
def stop_cephfs(connection, path='/mnt/cephfs', silent=False):
    '''Stops cephfs on remote machine. Does not return anything.'''
    remoto.process.check(connection, 'sudo fusermount -uz {}'.format(path), shell=True)


def start_cephfs(node, connection, ceph_deploypath, path='/mnt/cephfs', retries=5, silent=False):
    '''Starts cephFS on /mnt/cephfs.
    Warning: This function fails when cephfs is already mounted.
    Args:
        ceph_deploy: Path to `ceph-deploy` executable.
        mdss (iterable of `Info`): `Info` objects for nodes with metadata server designation. 

    Returns:
        `True` on success, `False` on failure.'''
    # if Designation.OSD in self.admin.designations:
    #     if subprocess.call('sudo cp ceph.bootstrap-osd.keyring /var/lib/ceph/bootstrap-osd/ceph.keyring', **get_subprocess_kwargs(silent)) != 0:
    #         return False
    remoto.process.check(connection, 'sudo mkdir -p {}'.format(path), shell=True)
    remoto.process.check(connection, 'sudo mkdir -p /etc/ceph'.format(path), shell=True)
    _, _, exitcode = remoto.process.check(connection, 'sudo apt update -y && sudo apt install ceph-fuse -y', shell=True)
    if exitcode != 0:
        return False

    # scp /etc/ceph/ceph.conf worker:/etc/ceph/ceph.conf
    # scp /etc/ceph/ceph.client.admin.keyring worker:/etc/ceph/ceph.client.admin.keyring
    if not send_config_with_keys([node], ceph_deploypath, silent):
        return False

    import time
    retries = retries+5 # We at least require a few tries before all servers respond.
    for x in range(retries):
        _, _, exitcode = remoto.process.check(connection, 'sudo ceph-fuse {}'.format(path), shell=True)
        if exitcode == 0:
            prints('[{}] Succesfully called ceph-fuse (attempt {}/{})'.format(node.hostname, x+1, retries))
            return True
        else:
            printw('[{}] Executing ceph-fuse... (attempt {}/{})'.format(node.hostname, x+1, retries))
        time.sleep(1)
    return False
