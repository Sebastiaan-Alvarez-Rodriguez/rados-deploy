from internal.remoto.modulegenerator import ModuleGenerator
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
import internal.util.fs as fs
import internal.util.importer as importer
from internal.util.printer import *


def _default_mountpoint_path():
    return '/mnt/cephfs'


def _stop_rados(remote_connection, module, reservation, mountpoint_path, silent=False):
    remote_module = remote_connection.import_module(module)
    return remote_module.stop_rados(str(reservation), mountpoint_path, silent)


def _generate_module_stop(silent=False):
    '''Generates RADOS-Ceph-start module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'stop_rados.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'executor.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'thirdparty', 'sshconf', 'sshconf.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'ssh_wrapper.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'util.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'designation.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'rados_util.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'config.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'pool.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'cephfs.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'manager.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'mds.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'monitor.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados', 'osd.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados_stop.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    import metareserve.reservation as reserve
    ModuleGenerator().with_modules(fs, reserve).with_files(*files).generate(generation_loc, allowed_imports=['remoto', 'remoto.process'], silent=True)
    return importer.import_full_path(generation_loc)


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


def stop(reservation, key_path=None, admin_id=None, mountpoint_path=_default_mountpoint_path(), silent=False):
    '''Boot RADOS-Ceph on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        mountpoint_path (optional str): Path where CephFS is mounted on all nodes.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.
        retries (optional int): Number of tries we try to perform potentially-crashing operations.

    Returns:
        `True` on success, `False` otherwise.'''
    if (not reservation) or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    admin_picked, _ = _pick_admin(reservation, admin=admin_id)
    print('Picked admin node: {}'.format(admin_picked))

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    connection = _get_ssh_connection(admin_picked.ip_public, silent=silent, ssh_params=ssh_kwargs)
    rados_module = _generate_module_stop()
    state_ok = _stop_rados(connection.connection, rados_module, reservation, mountpoint_path, silent=silent)
    if state_ok:
        prints('Stopping RADOS-Ceph succeeded.')
        return True
    else:
        printe('Stopping RADOS-Ceph failed on some nodes.')
        return False