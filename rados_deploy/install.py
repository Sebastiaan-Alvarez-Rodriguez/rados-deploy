import concurrent.futures
import subprocess
import tempfile

import internal.remoto.modules.rados_install as _rados_install
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
from internal.util.printer import *


def _install_rados(remote_connection, reservation, installdir, keypair, user, silent=False, cores=16):
    remote_connection.import_module(_rados_install)

    reservation_str = str(reservation)
    if install_ceph_deploy(installdir, silent=silent) and install_ceph(reservation_str, silent=silent) and install_rados(installdir, reservation_str, cores=cores, silent=silent):
        return True
    printe('There were problems during installation.')
    return False


def _make_keypair():
    '''Generates and returns a private-public keypar as a tuple(str, str).'''
    with tempfile.TemporaryDirectory() as dirname:
        subprocess.call('ssh-keygen -t rsa -b 4096 -f {} -N ""'.format(fs.join(dirname, 'tmp.rsa')))
        with open(fs.join(dirname, 'tmp.rsa'), 'r') as f:
            priv_key = ''.join(f.readlines())
        with open(fs.join(dirname, 'tmp.rsa.pub'), 'r') as f:
            pub_key = ''.join(f.readlines())
    return priv_key, pub_key


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


def _check_users(reservation):
    known_user = reservation.nodes[0].extra_info['user']
    return any(x for x in reservation.nodes[1:] if x.extra_info['user'] != known_user)


def _install_ssh(connection, reservation, keypair, user, use_sudo=True):
    connection.import_module(_ssh_install)
    return connection.install(str(reservation), keypair, user, use_sudo=use_sudo)


def install_ssh(reservation, key_path=None, cluster_keypair=None, silent=False, use_sudo=False):
    '''Installs ssh keys in the cluster for internal traffic.
    Warning: Requires that usernames on remote cluster nodes are equivalent.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to install Spark on.
        installdir (str): Location on remote host to install Spark in.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        cluster_keypair (optional tuple(str,str)): Keypair of (private, public) key to use for internal comms within the cluster. If `None`, a keypair will be generated.
        silent (optional bool): If set, does not print so much info.
    
    Returns:
        `True` on success, `False` otherwise.'''
    if not _check_users(reservation):
        printe('Found different usernames between nodes. All nodes must have the same user login!')
        return False
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': reservation.nodes[0].extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        internal_keypair = cluster_keypair
        if not internal_keypair:
            internal_keypair = _make_keypair()

        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=silent, ssh_params=ssh_kwargs) for x in reservation.nodes}
        connectionwrappers = {node: future.result() for node, future in futures_connection.items()}
        
        futures_ssh_install = {node: executor.submit(_install_ssh, connection, reservation, internal_keypair, user, use_sudo=use_sudo) for node, connection in connectionwrappers.items()}
        state_ok = True
        for node, ssh_future in futures_ssh_install.items():
            if not ssh_future.result():
                printe('Could not setup internal ssh key for node: {}'.format(node))
                state_ok = False
        return state_ok


def install(reservation, installdir, key_path=None, admin_id=None, cluster_keypair=None, silent=False, use_sudo=False, cores=16):
    '''Installs RADOS-ceph on remote cluster.
    Warning: Requires that usernames on remote cluster nodes are equivalent.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to install Spark on.
        installdir (str): Location on remote host to install Spark in.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id that must become the admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        cluster_keypair (optional tuple(str,str)): Keypair of (private, public) key to use for internal comms within the cluster. If `None`, a keypair will be generated.
        silent (optional bool): If set, does not print so much info.
        cores (optional int): Number of cores to compile RADOS-arrow with.
    
    Returns:
        `True` on success, `False` otherwise.'''
    if not _check_users(reservation):
        printe('Found different usernames between nodes. All nodes must have the same user login!')
        return False

    admin = _pick_admin(reservation, admin=admin_id)

    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    if not install_ssh(reservation, key_path, cluster_keypair, silent=silent, use_sudo=use_sudo):
        return False

    connection = _get_ssh_connection(admin.ip_public, silent=silent, ssh_params=ssh_kwargs)
    return _install_rados(connection, reservation, installdir, internal_keypair, admin.extra_info['user'], silent=silent, cores=cores)