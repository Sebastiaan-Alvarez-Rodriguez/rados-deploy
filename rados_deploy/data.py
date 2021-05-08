import concurrent.futures
from multiprocessing import cpu_count

import remoto.process

from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.util.fs as fs
from internal.util.printer import *


def _default_stripe():
    return 64*1024*1024 # 64MB


def _default_mountpoint_path():
    return '/mnt/cephfs'



def _prepare_remote_file(connection, stripe, source_file, dest_file):
    remoto.process.check(connection, 'sudo mkdir -p {}'.format(fs.dirname(dest_file)), shell=True)
    _, _, exitcode = remoto.process.check(connection, 'sudo touch {}'.format(dest_file), shell=True)
    if exitcode != 0:
        printe('Could not touch file at cluster: {}'.format(dest_file))
        return False
    cmd = 'sudo setfattr -n ceph.file.layout.object_size -v {} {}'.format(stripe, dest_file)
    _, _, exitcode = remoto.process.check(connection, cmd, shell=True)
    if exitcode != 0:
        printe('Could not stripe file at cluster: {}'.format(cmd))
        return False
    return True


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


def deploy(reservation, key_path, paths, stripe=_default_stripe(), admin_id=None, mountpoint_path=_default_mountpoint_path(), silent=False):
    '''Deploy data on the RADOS-Ceph cluster, on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        paths (list(str)): Data paths to offload to the remote cluster. Can be relative to CWD or absolute.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        mountpoint_path (optional str): Path where CephFS is mounted on all nodes.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.
        retries (optional int): Number of tries we try to perform potentially-crashing operations.

    Returns:
        `True` on success, `False` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))
    if not any(paths):
        return True

    admin_picked, _ = _pick_admin(reservation, admin=admin_id)
    print('Picked admin node: {}'.format(admin_picked))

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    connection = _get_ssh_connection(admin_picked.ip_public, silent=True, ssh_params=ssh_kwargs)

    paths = [fs.abspath(x) for x in paths]

    _, _, exitcode = remoto.process.check(connection.connection, 'which setfattr', shell=True)
    if exitcode != 0:
        process.check(connection.connection, 'sudo apt install attr -y', shell=True)

    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
        prepare_futures = []

        for path in paths:
            if fs.isfile(path):
                prepare_futures.append(executor.submit(_prepare_remote_file, connection.connection, stripe, path, fs.join(mountpoint_path, fs.basename(path))))
            elif fs.isdir(path):
                to_visit = [path]
                path_len = len(path)
                while any(to_visit):
                    visit_now = to_visit.pop()
                    visit_now_len = len(visit_now)
                    to_visit += list(fs.ls(visit_now, only_dirs=True, full_paths=True))
                    prepare_futures += [executor.submit(_prepare_remote_file, connection.connection, stripe, x, fs.join(mountpoint_path, x[path_len+1:])) for x in fs.ls(visit_now, only_files=True, full_paths=True)]
        if not all(x.result() for x in prepare_futures):
            return False

        if not silent:
            print('Transferring data...')
        fun = lambda path: subprocess.check_call('rsync -e "ssh -F {}" -az {} {}:{}'.format(connection.ssh_config.name, path, admin_picked.ip_public, fs.join(mountpoint_path, fs.basename(path))))
        rsync_futures = [executor.submit(fun, path) for path in paths]

        if all(x.result() for x in prepare_futures):
            prints('Data deployed.')
            return True
        else:
            printe('Could not deploy data.')
            return False