from rados_deploy import start as _start
from rados_deploy import stop as _stop
from rados_deploy.internal.util.printer import *
import rados_deploy.internal.defaults.start as start_defaults
import rados_deploy.internal.defaults.restart as defaults



def restart(reservation, key_path=None, admin_id=None, mountpoint_path=start_defaults.mountpoint_path(), silent=False, retries=defaults.retries()):
    '''Boot RADOS-Ceph on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.
        retries (optional int): Number of tries we try to perform potentially-crashing operations.

    Returns:
        `(True, admin_node_id)` on success, `(False, None)` otherwise.'''
    if not _stop(reservation, key_path, admin_id, mountpoint_path, silent):
        return False
    retval, node_id = _start(reservation, key_path, admin_id, mountpoint_path, silent, retries)
    if retval:
        prints('Restarting RADOS-Ceph succeeded.')
    else:
        printe('Restarting RADOS-Ceph failed.')
    return retval, node_id