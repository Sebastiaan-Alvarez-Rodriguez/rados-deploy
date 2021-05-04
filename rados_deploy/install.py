import concurrent.futures
import hashlib
import subprocess
import tempfile

from designation import Designation
import internal.remoto.modules.ssh_install as _ssh_install
import internal.remoto.modules.rados_install as _rados_install
from internal.remoto.util import get_ssh_connection as _get_ssh_connection
from internal.util.printer import *


def _default_cores():
    return 16

def _default_retries():
    return 5

def _default_use_sudo():
    return False

def _install_rados(connection, reservation, installdir, silent=False, cores=_default_cores()):
    remote_module = connection.import_module(_rados_install)

    hosts = [x.hostname for x in reservation.nodes]
    if not remote_module.install_ceph_deploy(installdir, silent):
        printe('Could not install ceph-deploy.')
        return False
    if not remote_module.install_ceph({x.hostname: [str(Designation[y.strip().upper()]) for y in x.extra_info['designations'].split(',')] if 'designations' in x.extra_info else [] for x in reservation.nodes}, silent):
        printe('Could not install ceph on some node(s).')
        return False
    if not remote_module.install_rados(installdir, hosts, silent, cores):
        printe('Could not install RADOS-ceph on some node(s).')
        return False
    prints('Installed RADOS-ceph.')
    return True


def _make_keypair():
    '''Generates and returns a private-public keypar as a tuple(str, str).'''
    with tempfile.TemporaryDirectory() as dirname:
        subprocess.call('ssh-keygen -t rsa -b 4096 -f {} -N ""'.format(os.path.join(dirname, 'tmp.rsa')), shell=True)
        with open(os.path.join(dirname, 'tmp.rsa'), 'r') as f:
            priv_key = ''.join(f.readlines())
        with open(os.path.join(dirname, 'tmp.rsa.pub'), 'r') as f:
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
    '''Checks if all usernames are the same. Returns `True` if all usernames are equivalent, `False` otherwise.'''
    nodes = list(reservation.nodes)
    known_user = nodes[0].extra_info['user']
    return not any(x for x in nodes[1:] if x.extra_info['user'] != known_user)


def _installed_ssh(connection, keypair=None):
    remote_module = connection.import_module(_ssh_install)
    privkey_sha256 = hashlib.sha256(bytes(keypair[0])).hexdigest() if keypair else None
    return remote_module.already_installed(privkey_sha256)


def _install_ssh(connection, reservation, keypair, user, use_sudo=True):
    remote_module = connection.import_module(_ssh_install)
    return remote_module.install_ssh_keys([x.hostname for x in reservation.nodes], keypair, user, use_sudo)


def install_ssh(reservation, key_path=None, cluster_keypair=None, silent=False, use_sudo=_default_use_sudo()):
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
    user = list(reservation.nodes)[0].extra_info['user']
    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': user, 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        futures_connection = {x: executor.submit(_get_ssh_connection, x.ip_public, silent=silent, ssh_params=ssh_kwargs) for x in reservation.nodes}
        connectionwrappers = {node: future.result() for node, future in futures_connection.items()}
        
        futures_ssh_installed = {node: executor.submit(_installed_ssh, wrapper.connection, keypair=cluster_keypair) for node, wrapper in connectionwrappers.items()}
        do_install = False
        for node, ssh_future in futures_ssh_installed.items():
            if not ssh_future.result():
                print('SSH keys not installed in node: {}'.format(node))
                do_install = True
        if do_install:
            internal_keypair = cluster_keypair
            if not internal_keypair:
                internal_keypair = _make_keypair()
            futures_ssh_install = {node: executor.submit(_install_ssh, wrapper.connection, reservation, internal_keypair, user, use_sudo=use_sudo) for node, wrapper in connectionwrappers.items()}
            state_ok = True
            for node, ssh_future in futures_ssh_install.items():
                if not ssh_future.result():
                    printe('Could not setup internal ssh key for node: {}'.format(node))
                    state_ok = False
            return state_ok
        prints('SSH keys already installed.')
        return True


def install(reservation, installdir, key_path=None, admin_id=None, cluster_keypair=None, silent=False, use_sudo=_default_use_sudo(), cores=_default_cores()):
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

    admin_picked, _ = _pick_admin(reservation, admin=admin_id)
    print('Picked admin node: {}'.format(admin_picked))
        
    ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
    if key_path:
        ssh_kwargs['IdentityFile'] = key_path

    if not install_ssh(reservation, key_path, cluster_keypair, silent=silent, use_sudo=use_sudo):
        return False

    connection = _get_ssh_connection(admin_picked.ip_public, silent=silent, ssh_params=ssh_kwargs)
    return _install_rados(connection.connection, reservation, installdir, silent=silent, cores=cores)