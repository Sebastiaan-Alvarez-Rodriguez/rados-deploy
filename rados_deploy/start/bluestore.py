from rados_deploy import Designation
import rados_deploy.internal.defaults.start as defaults
from rados_deploy.internal.remoto.modulegenerator import ModuleGenerator
from rados_deploy.internal.remoto.ssh_wrapper import get_wrapper, close_wrappers
from rados_deploy.internal.util.byteconverter import to_bytes
import rados_deploy.internal.util.fs as fs
import rados_deploy.internal.util.importer as importer
from rados_deploy.internal.util.printer import *

from rados_deploy.start._internal import _pick_admin as _internal_pick_admin
from rados_deploy.start._internal import _compute_placement_groups as _internal_compute_placement_groups


def _start_rados(remote_connection, module, reservation, mountpoint_path, osd_op_threads, osd_pool_size, osd_max_obj_size, placement_groups, use_client_cache, use_ceph_volume, silent=False, retries=5):
    remote_module = remote_connection.import_module(module)
    return remote_module.start_rados_bluestore(str(reservation), mountpoint_path, osd_op_threads, osd_pool_size, osd_max_obj_size, placement_groups, use_client_cache, use_ceph_volume, silent, retries)


def _generate_module_start(silent=False):
    '''Generates RADOS-Ceph-start module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'generated', 'start_rados_bluestore.py')
    files = [
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'util', 'executor.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'thirdparty', 'sshconf', 'sshconf.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'ssh_wrapper.py'),
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
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'start', 'bluestore.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    import metareserve.reservation as reserve
    ModuleGenerator().with_modules(fs, reserve).with_files(*files).generate(generation_loc, allowed_imports=['remoto', 'remoto.process'], silent=True)
    return importer.import_full_path(generation_loc)


def bluestore(reservation, key_path=None, admin_id=None, connectionwrapper=None, mountpoint_path=defaults.mountpoint_path(), osd_op_threads=defaults.osd_op_threads(), osd_pool_size=defaults.osd_pool_size(), osd_max_obj_size=defaults.osd_max_obj_size(), placement_groups=None, use_client_cache=True, device_path=None, use_ceph_volume=False, silent=False, retries=defaults.retries()):
    '''Boot RADOS-Ceph on an existing reservation, running bluestore.
    Requires either a "device_path" key to be set in the extra info of all OSD nodes, or the "device_path" parameter must be set.
    Should point to device to use with bluestore on all nodes.
    Args:
        reservation (metareserve.Reservation): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        connectionwrapper (optional RemotoSSHWrapper): If set, uses given connection, instead of building a new one.
        mountpoint_path (optional str): Path where CephFS will be mounted on all nodes.
        osd_op_threads (optional int): Number of op threads to use for each OSD. Make sure this number is not greater than the amount of cores each OSD has.
        osd_pool_size (optional int): Fragmentation of object to given number of OSDs. Must be less than or equal to amount of OSDs.
        osd_max_obj_size (int): Maximal object size in bytes. Normal=128*1024*1024 (128MB).
        placement_groups (optional int): Amount of placement groups in Ceph. If not set, we use the recommended formula `(num osds * 100) / (pool size`, as found here: https://ceph.io/pgcalc/.
        use_client_cache (bool): Toggles using cephFS I/O cache.
        device_path (optional str): If set, overrides the "device_path" extra info for all nodes with given value. Should point to device to use with bluestore on all nodes.
        use_ceph_volume (bool): If set, uses 'ceph-volume' instead of 'osd create'
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.
        retries (optional int): Number of tries we try to perform potentially-crashing operations.

    Returns:
        `(True, admin_node_id)` on success, `(False, None)` otherwise.'''
    if not reservation or len(reservation) == 0:
        raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))

    if isinstance(placement_groups, int):
        if placement_groups < 1:
            raise ValueError('Amount of placement groups must be higher than zero!')
    else: # We assume `placememt_groups = None`
        placement_groups = _internal_compute_placement_groups(reservation=reservation)

    if device_path: # We got an overriding device_path value
        for x in reservation.nodes:
            if 'designations' in x.extra_info and Designation.OSD.name.lower() in x.extra_info['designations'].split(','):
                x.extra_info['device_path'] = device_path
    else:
        if any(True for x in reservation.nodes if 'designations' in x.extra_info and Designation.OSD.name.lower() in x.extra_info['designations'].split(',') and not 'device_path' in x.extra_info): # We lack at least 1 device_path value.
            printe('Missing "device_path" specifier on the following nodes:\n{}'.format('\n'.join('\t{}'.format(x) for x in reservation.nodes if 'designations' in x.extra_info and Designation.OSD.name.lower() in x.extra_info['designations'].split(',') and not 'device_path' in x.extra_info)))
            return False, None

    admin_picked, _ = _internal_pick_admin(reservation, admin=admin_id)
    printc('Picked admin node: {}'.format(admin_picked), Color.CAN)

    local_connections = connectionwrapper == None

    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrapper = get_wrapper(admin_picked, admin_picked.ip_public, silent=silent, ssh_params=ssh_kwargs)
    rados_module = _generate_module_start()
    state_ok = _start_rados(connectionwrapper.connection, rados_module, reservation, mountpoint_path, osd_op_threads, osd_pool_size, osd_max_obj_size, placement_groups, use_client_cache, use_ceph_volume, silent=silent, retries=retries)

    if local_connections:
        close_wrappers([connectionwrapper])

    if state_ok:
        prints('Started RADOS-Ceph succeeded.')
        return True, admin_picked.node_id
    else:
        printe('Starting RADOS-Ceph failed on some nodes.')
        return False, None