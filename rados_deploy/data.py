import concurrent.futures
from multiprocessing import cpu_count
import os
import subprocess

import remoto.process

import rados_deploy.internal.defaults.start as start_defaults
import rados_deploy.internal.defaults.data as defaults
from rados_deploy.internal.remoto.ssh_wrapper import get_wrapper, close_wrappers
import rados_deploy.internal.util.fs as fs
import rados_deploy.internal.util.importer
from rados_deploy.internal.util.printer import *



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


def clean(reservation, paths, key_path=None, admin_id=None, connectionwrapper=None, mountpoint_path=start_defaults.mountpoint_path(), silent=False):
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

    local_connections = connectionwrapper == None
    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        connectionwrapper = get_wrapper(admin_picked.ip_public, silent=True, ssh_params=ssh_kwargs)

    if not any(paths):
        _, _, exitcode = remoto.process.check(connectionwrapper.connection, 'sudo rm -rf {}/*'.format(mountpoint_path), shell=True)
        state_ok = exitcode == 0
    else:
        paths = [x if x[0] != '/' else x[1:] for x in paths]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
            if not silent:
                print('Deleting data...')
            futures_rm = [executor.submit(remoto.process.check, connectionwrapper.connection, 'sudo rm -rf {}'.format(fs.join(mountpoint_path, path)), shell=True) for path in paths]

            state_ok = all(x.result()[2] == 0 for x in futures_rm)

    if state_ok:
        prints('Data deleted.')
    else:
        printe('Could not delete data.')
    if local_connections:
        close_wrappers([connectionwrapper])
    return state_ok



def deploy(reservation, paths=None, key_path=None, admin_id=None, connectionwrapper=None, stripe=defaults.stripe(), copy_multiplier=1, link_multiplier=1, mountpoint_path=start_defaults.mountpoint_path(), silent=False):
    '''Deploy data on remote RADOS-Ceph clusters, on an existing reservation.
    Dataset sizes can be inflated on the remote, using 2 strategies:
     1. link multiplication: Every dataset file receives `x` hardlinks.
        The hardlinks ensure the dataset size appears to be `x` times larger, but in reality, just the original file consumes space.
        This method is very fast, but has drawbacks: Only the original files are stored by Ceph.
        When using the RADOS-Arrow connector, this means Arrow will spam only the nodes that contain the original data.
        E.g: If we deploy 1 file of 64MB, with link multiplier 1024, the data will apear to be 64GB.
             The storage space used on RADOS-Ceph will still be 64MB, because we have 1 real file of 64MB, and 1023 hardlinks to that 1 file.
             The actual data is only stored on 3 OSDs (with default Ceph Striping factor 3).
             Now, Arrow will spam all work to those 3 OSDs containing the data, while the rest is idle.
     2. file multiplication: Every dataset file receives `x` copies.
        This method is slower than the one listed above, because real data has to be copied. 
        It also actually increases storage usage, contrary to above. 
        However, because we multiply real data, the load is guaranteed to be balanced across nodes, as far as Ceph does that.

    Note that mutiple multiplication techniques can be combined, in which case they stack.
    E.g: If we deploy 1 file of 64MB, with a copy multiplier 4 and a link multiplier 1024, we get 4 real files (1 original + 3 copies),
         and each file gets 1023 hardlinks assigned to it.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        connectionwrapper (optional RemotoSSHWrapper): If set, uses given connection, instead of building a new one.
        paths (optional list(str)): Data paths to offload to the remote cluster. Can be relative to CWD or absolute.
        stripe (optional int): Ceph object stripe property, in megabytes.
        copy_multiplier (optional int): If set to a value `x`, makes the dataset appear `x` times larger by copying every file `x`-1 times. Does nothing if `x`<=1.
        link_multiplier (optional int): If set to a value `x`, makes the dataset appear `x` times larger by adding `x`-1 hardlinks for every transferred file. Does nothing if `x`<=1.
        mountpoint_path (optional str): Path where CephFS is mounted on all nodes.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.

    Returns:
        `True` on success, `False` otherwise.'''
    module = importer.import_full_path(fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'data_deploy', 'rados_deploy.deploy.plugin.py'))
    args = []
    kwargs = {'admin_id': admin_id, 'connectionwrapper': connectionwrapper, 'stripe': stripe, 'copy_multiplier': copy_multiplier, 'link_multiplier': link_multiplier}
    return module.execute(reservation, key_path, paths, dest, silent, *args, **kwargs)


def generate(reservation, key_path=None, admin_id=None, cmd=None, paths=None, stripe=defaults.stripe(), multiplier=1, mountpoint_path=start_defaults.mountpoint_path(), silent=False):
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