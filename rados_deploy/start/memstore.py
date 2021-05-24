import rados_deploy.internal.defaults.start as defaults
from rados_deploy.internal.remoto.modulegenerator import ModuleGenerator
from rados_deploy.internal.remoto.util import get_ssh_connection as _get_ssh_connection
from rados_deploy.internal.util.byteconverter import to_bytes
import rados_deploy.internal.util.fs as fs
import rados_deploy.internal.util.importer as importer
from rados_deploy.internal.util.printer import *

import rados_deploy.start._internal as _internal


def _start_rados(remote_connection, module, reservation, mountpoint_path, osd_op_threads, osd_pool_size, storage_size, silent=False, retries=5):
    remote_module = remote_connection.import_module(module)
    return remote_module.start_rados_memstore(str(reservation), mountpoint_path, osd_op_threads, osd_pool_size, storage_size, silent, retries)


def _generate_module_start(silent=False):
    '''Generates RADOS-Ceph-start module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'generated', 'start_rados.py')
    files = [
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'util', 'executor.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'thirdparty', 'sshconf', 'sshconf.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'ssh_wrapper.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'util.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'designation.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'storagetype.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'env.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'rados_util.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'config.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'pool.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'cephfs.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'manager.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'mds.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'monitor.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'rados', 'osd.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'start', 'shared.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'start', 'memstore.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    import metareserve.reservation as reserve
    ModuleGenerator().with_modules(fs, reserve).with_files(*files).generate(generation_loc, allowed_imports=['remoto', 'remoto.process'], silent=True)
    return importer.import_full_path(generation_loc)


def memstore(reservation, key_path=None, admin_id=None, mountpoint_path=defaults.mountpoint_path(), osd_op_threads=defaults.osd_op_threads(), osd_pool_size=defaults.osd_pool_size(), storage_size=defaults.memstore_storage_size(), silent=False, retries=defaults.retries()):
    '''Boot RADOS-Ceph on an existing reservation, running memstore.
    Args:
        reservation (metareserve.Reservation): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        mountpoint_path (optional str): Path where CephFS will be mounted on all nodes.
        osd_op_threads (optional int): Number of op threads to use for each OSD. Make sure this number is not greater than the amount of cores each OSD has.
        osd_pool_size (optional int): Fragmentation of object to given number of OSDs. Must be less than or equal to amount of OSDs.
        storage_size (optional str): Amount of bytes of RAM to allocate on each node. Value must use size indicator B, KiB, MiB, GiB, TiB.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.
        retries (optional int): Number of tries we try to perform potentially-crashing operations.

    Returns:
        `(True, admin_node_id)` on success, `(False, None)` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    admin_picked, _ = _internal._pick_admin(reservation, admin=admin_id)
    printc('Picked admin node: {}'.format(admin_picked), Color.CAN)

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    connection = _get_ssh_connection(admin_picked.ip_public, silent=silent, ssh_params=ssh_kwargs)
    rados_module = _generate_module_start()
    state_ok = _start_rados(connection.connection, rados_module, reservation, mountpoint_path, osd_op_threads, osd_pool_size, storage_size, silent=silent, retries=retries)
    if state_ok:
        prints('Started RADOS-Ceph succeeded.')
        return True, admin_picked.node_id
    else:
        printe('Starting RADOS-Ceph failed on some nodes.')
        return False, None