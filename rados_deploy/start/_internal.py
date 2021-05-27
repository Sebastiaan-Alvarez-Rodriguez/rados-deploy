import math

from rados_deploy import Designation


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


def _compute_placement_groups(num_osds=None, reservation=None, num_pools=3):
    if num_osds == None and reservation == None:
        raise ValueError('Either need number of osds or reservation for computing placement groups.')
    if not num_osds:
        num_osds = counted_total_osds = sum([sum(1 for y in x.extra_info['designations'].split(',') if y == Designation.OSD.name.lower()) for x in osds])
    num_pgs = (num_osds * 100) / num_pools

    pow2_pg = 2**(math.ceil(num_pgs/2)-1).bit_length()

    if pow2_pg < num_pgs/4*3: # We are more than 25% away from our target, pick larger PG number
        pow2_pg *= 2
    return pow2_pg