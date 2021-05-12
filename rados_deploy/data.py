import concurrent.futures
from multiprocessing import cpu_count
import os

import remoto.process

from rados_deploy.internal.remoto.util import get_ssh_connection as _get_ssh_connection
import rados_deploy.internal.util.fs as fs
from rados_deploy.internal.util.printer import *


def _default_stripe():
    return 64 # 64MB


def _default_mountpoint_path():
    return '/mnt/cephfs'


def _prepare_remote_file(connection, stripe, links_amount, source_file, dest_file):
    remoto.process.check(connection, 'sudo mkdir -p {}'.format(fs.dirname(dest_file)), shell=True)
    _, _, exitcode = remoto.process.check(connection, 'sudo touch {}'.format(dest_file), shell=True)
    if exitcode != 0:
        printe('Could not touch file at cluster: {}'.format(dest_file))
        return False

    exitcodes = [remoto.process.check(connection, 'sudo ln {0} {0}.{1}'.format(dest_file, x), shell=True)[2] for x in range(links_amount)]
    if any(x for x in exitcodes if x != 0):
        printe('Could not add hardlinks for file: {}'.format(dest_file))
        return False

    _, _, exitcode = remoto.process.check(connection, 'sudo setfattr -n ceph.file.layout.object_size -v {} {}'.format(stripe*1024*1024, dest_file), shell=True)
    if exitcode != 0:
        printe('Could not stripe file at cluster: {}. Is the cluster running?'.format(dest_file))
        return False
    return True


def _ensure_attr(connection):
    '''Installs the 'attr' package, if not available.'''
    _, _, exitcode = remoto.process.check(connection, 'which setfattr', shell=True)
    if exitcode != 0:
        remoto.process.check(connection, 'sudo apt install attr -y', shell=True)


def _pick_admin(reservation, admin=None):
    '''Picks a ceph admin node.
    Args:
        reservation (`metareserve.Reservation`): Reservation object to pick admin from.
        admin (optional int): If set, picks node with given `node_id`. Picks node with lowest public ip value, otherwise.

    Returns:
        admin, list of non-admins.'''
    if len(reservation) == 1:
        return next(reservation.nodes), []

    if admin:
        return reservation.get_node(node_id=admin), [x for x in reservation.nodes if x.node_id != admin]
    else:
        tmp = sorted(reservation.nodes, key=lambda x: x.ip_public)
        return tmp[0], tmp[1:]


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def clean(reservation, paths, key_path=None, admin_id=None, mountpoint_path=_default_mountpoint_path(), silent=False):
    '''Cleans data from the RADOS-Ceph cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        paths (list(str)): Data paths to delete to the remote cluster. Mountpoint path is always prepended.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        mountpoint_path (optional str): Path where CephFS is mounted on all nodes.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.

    Returns:
        `True` on success, `False` otherwise.'''
    if (not reservation) or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))
    
    admin_picked, _ = _pick_admin(reservation, admin=admin_id)
    print('Picked admin node: {}'.format(admin_picked))

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    connection = _get_ssh_connection(admin_picked.ip_public, silent=True, ssh_params=ssh_kwargs)

    if not any(paths):
        _, _, exitcode = remoto.process.check(connection.connection, 'sudo rm -rf {}/*'.format(mountpoint_path), shell=True)
        state_ok = exitcode == 0
    else:
        paths = [x if x[0] != '/' else x[1:] for x in paths]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
            if not silent:
                print('Deleting data...')
            rm_futures = [executor.submit(remoto.process.check, connection.connection, 'sudo rm -rf {}'.format(fs.join(mountpoint_path, path)), shell=True) for path in paths]

            state_ok = all(x.result()[2] == 0 for x in rm_futures)

    if state_ok:
        prints('Data deleted.')
    else:
        printe('Could not delete data.')
    return state_ok



def deploy(reservation, paths=None, key_path=None, admin_id=None, stripe=_default_stripe(), multiplier=1, mountpoint_path=_default_mountpoint_path(), silent=False):
    '''Deploy data on the RADOS-Ceph cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        paths (optional list(str)): Data paths to offload to the remote cluster. Can be relative to CWD or absolute.
        stripe (optional int): Ceph object stripe property, in megabytes.
        multiplier (optional int): If set to a value `x`, makes the dataset appear `x` times larger by adding `x`-1 hardlinks for every transferred file. Does nothing if `x`<=1.
        mountpoint_path (optional str): Path where CephFS is mounted on all nodes.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))
    if stripe < 4:
        raise ValueError('Stripe size must be equal to or greater than 4MB (and a multiple of 4MB)!')
    if stripe % 4 != 0:
        raise ValueError('Stripe size must be a multiple of 4MB!')
    if not any(paths):
        return True

    admin_picked, _ = _pick_admin(reservation, admin=admin_id)
    print('Picked admin node: {}'.format(admin_picked))

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    connection = _get_ssh_connection(admin_picked.ip_public, silent=True, ssh_params=ssh_kwargs)

    paths = [fs.abspath(x) for x in paths]

    _ensure_attr(connection.connection)

    max_filesize = stripe * 1024 * 1024
    links_to_add = max(1, multiplier) - 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
        prepare_futures = []

        for path in paths:
            if fs.isfile(path):
                if os.path.getsize(path) > max_filesize:
                    printe('File {} is too large ({} bytes, max allowed is {} bytes)'.format(path, os.path.getsize(path), max_filesize))
                    return False
                prepare_futures.append(executor.submit(_prepare_remote_file, connection.connection, stripe, links_to_add, path, fs.join(mountpoint_path, fs.basename(path))))
            elif fs.isdir(path):
                to_visit = [path]
                path_len = len(path)
                while any(to_visit):
                    visit_now = to_visit.pop()
                    to_visit += list(fs.ls(visit_now, only_dirs=True, full_paths=True))
                    files = list(fs.ls(visit_now, only_files=True, full_paths=True))
                    files_too_big = [x for x in files if os.path.getsize(x) > max_filesize]
                    if any(files_too_big):
                        for x in files_too_big:
                            printe('File {} is too large ({} bytes, max allowed is {} bytes)'.format(x, os.path.getsize(x), max_filesize))
                        return False
                    prepare_futures += [executor.submit(_prepare_remote_file, connection.connection, stripe, links_to_add, x, fs.join(mountpoint_path, x[path_len+1:])) for x in files]
        if not all(x.result() for x in prepare_futures):
            return False

        if not silent:
            print('Transferring data...')
        fun = lambda path: subprocess.call('rsync -e "ssh -F {}" -az {} {}:{}'.format(connection.ssh_config.name, path, admin_picked.ip_public, fs.join(mountpoint_path, fs.basename(path))), shell=True) == 0
        rsync_futures = [executor.submit(fun, path) for path in paths]

        if all(x.result() for x in rsync_futures):
            prints('Data deployed.')
            return True
        else:
            printe('Could not deploy data.')
            return False



def generate(reservation, key_path=None, admin_id=None, cmd=None, paths=None, stripe=_default_stripe(), multiplier=1, mountpoint_path=_default_mountpoint_path(), silent=False):
    '''Deploy data on the RADOS-Ceph cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        cmd (optional str): Command to execute on the remote cluster to generate the data.
        paths (optional list(str)): Data paths to offload to the remote cluster. Can be relative to CWD or absolute.
        stripe (optional int): Ceph object stripe property, in megabytes.
        multiplier (optional int): If set to a value `x`, makes the dataset appear `x` times larger by adding `x`-1 hardlinks for every transferred file. Does nothing if `x`<=1.
        mountpoint_path (optional str): Path where CephFS is mounted on all nodes.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))
    if stripe < 4:
        raise ValueError('Stripe size must be equal to or greater than 4MB (and a multiple of 4MB)!')
    if stripe % 4 != 0:
        raise ValueError('Stripe size must be a multiple of 4MB!')
    if not cmd:
        raise ValueError('Command to generate data not provided.')
    raise NotImplementedError
    return True