import start
import stop
from internal.util.printer import *


def _default_retries():
    return start._default_retries()


def _default_mountpoint_path():
    return start._default_mountpoint_path()


def restart(reservation, key_path=None, admin_id=None, mountpoint_path=_default_mountpoint_path(), silent=False, retries=_default_retries()):
    '''Boot RADOS-Ceph on an existing reservation.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to start RADOS-Ceph on.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id of the ceph admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        silent (optional bool): If set, we only print errors and critical info. Otherwise, more verbose output.
        retries (optional int): Number of tries we try to perform potentially-crashing operations.

    Returns:
        `True` on success, `False` otherwise.'''
    if not stop.stop(reservation, key_path, admin_id, mountpoint_path, silent):
        return False
    if start.start(reservation, key_path, admin_id, mountpoint_path, silent, retries):
        prints('Restarting RADOS-Ceph succeeded.')
        return True
    else:
        printe('Restarting RADOS-Ceph failed.')
        return False