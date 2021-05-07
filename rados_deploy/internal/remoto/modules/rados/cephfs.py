import subprocess

'''Utility functions to interact with CephFS.
Requires:
    rados_util'''
def stop_cephfs(path='/mnt/cephfs', silent=False):
    '''Stops cephfs on the machine this function is executed. Does not return anything.'''
    subprocess.call('sudo fusermount -uz {}'.format(path), **get_subprocess_kwargs(silent))


def start_cephfs(path='/mnt/cephfs', retries=5, silent=False):
    '''Starts cephFS on /mnt/cephfs.
    Warning: This function fails when cephfs is already mounted.
    Args:
        ceph_deploy: Path to `ceph-deploy` executable.
        mdss (iterable of `Info`): `Info` objects for nodes with metadata server designation. 

    Returns:
        `True` on success, `False` on failure.'''
    if Designation.OSD in self.admin.designations:
        if subprocess.call('sudo cp ceph.bootstrap-osd.keyring /var/lib/ceph/bootstrap-osd/ceph.keyring', **get_subprocess_kwargs(silent)) != 0:
            return False

    if subprocess.call('sudo mkdir -p {}'.format(path), **get_subprocess_kwargs(silent)) != 0:
        return False

    if subprocess.check_call('sudo apt install ceph-fuse -y', **get_subprocess_kwargs(silent)) != 0:
        return False

    import time
    retries = retries+5 # We at least require a few tries before all servers respond.
    for x in range(retries):
        if subprocess.call('sudo ceph-fuse {}'.format(path), **get_subprocess_kwargs(silent)) == 0:
            prints('Succesfully called ceph-fuse (attempt {}/{})'.format(x+1, retries))
            return True
        else:
            printw('Executing ceph-fuse... (attempt {}/{})'.format(x+1, retries))
        time.sleep(1)
    return False
