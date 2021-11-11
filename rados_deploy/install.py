import concurrent.futures
import hashlib
import subprocess
import tempfile

from rados_deploy import Designation
import rados_deploy.internal.defaults.install as defaults
from rados_deploy.internal.remoto.modulegenerator import ModuleGenerator
from rados_deploy.internal.remoto.ssh_wrapper import get_wrapper, get_wrappers, close_wrappers
import rados_deploy.internal.util.fs as fs
import rados_deploy.internal.util.importer as importer
import rados_deploy.internal.util.location as loc
from rados_deploy.internal.util.printer import *


def _install_rados(connection, module, reservation, install_dir, arrow_url=defaults.arrow_url(), force_reinstall=False, debug=False, silent=False, cores=defaults.cores()):
    remote_module = connection.import_module(module)

    hosts = [x.hostname for x in reservation.nodes]
    if not remote_module.install_ceph_deploy(loc.cephdeploydir(install_dir), silent):
        printe('Could not install ceph-deploy.')
        return False
    hosts_designations_mapping = {x.hostname: [Designation[y.strip().upper()].name for y in x.extra_info['designations'].split(',')] if 'designations' in x.extra_info else [] for x in reservation.nodes}
    print(hosts_designations_mapping)
    if not remote_module.install_ceph(hosts_designations_mapping, silent):
        printe('Could not install Ceph on some node(s).')
        return False
    if not remote_module.install_rados(loc.arrowdir(install_dir), hosts_designations_mapping, arrow_url, force_reinstall, debug, silent, cores):
        printe('Could not install RADOS-Ceph on some node(s).')
        return False
    prints('Installed RADOS-Ceph.')
    return True


def _installed_ssh(connection, module, keypair=None):
    remote_module = connection.import_module(module)
    privkey_sha256 = hashlib.sha256(bytes(keypair[0])).hexdigest() if keypair else None
    return remote_module.already_installed(privkey_sha256)


def _install_ssh(connection, module, reservation, keypair, user, use_sudo=True):
    remote_module = connection.import_module(module)
    return remote_module.install_ssh_keys([x.hostname for x in reservation.nodes], keypair, user, use_sudo)


def _generate_module_ssh(silent=False):
    '''Generates SSH-install module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'install_ssh.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'ssh_install.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_module(fs).with_files(*files).generate(generation_loc, silent)
    return importer.import_full_path(generation_loc)


def _generate_module_rados(silent=False):
    '''Generates RADOS-arrow-install module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'install_rados.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'executor.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'env.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'rados_install.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_modules(fs, importer).with_files(*files).generate(generation_loc, silent)
    return importer.import_full_path(generation_loc)


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


def install_ssh(reservation, connectionwrappers=None, key_path=None, cluster_keypair=None, silent=False, use_sudo=defaults.use_sudo()):
    '''Installs ssh keys in the cluster for internal traffic.
    Warning: Requires that usernames on remote cluster nodes are equivalent.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to install RADOS-Ceph on.
        connectionwrappers (optional dict(metareserve.Node, RemotoSSHWrapper)): If set, uses given connections, instead of building new ones.
        install_dir (str): Location on remote host to install RADOS-Ceph in.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        cluster_keypair (optional tuple(str,str)): Keypair of (private, public) key to use for internal comms within the cluster. If `None`, a keypair will be generated.
        silent (optional bool): If set, does not print so much info.

    Returns:
        `True` on success, `False` otherwise.'''
    if not _check_users(reservation):
        printe('Found different usernames between nodes. All nodes must have the same user login!')
        return False
    user = list(reservation.nodes)[0].extra_info['user']
    
    local_connections = connectionwrappers == None

    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': user, 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrappers = get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=ssh_kwargs, silent=silent)
    else:
        if not all(x.open for x in connectionwrappers):
            raise ValueError('SSH installation failed: At least one connection is already closed.')

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_module = _generate_module_ssh()

        futures_ssh_installed = {node: executor.submit(_installed_ssh, wrapper.connection, ssh_module, keypair=cluster_keypair) for node, wrapper in connectionwrappers.items()}
        do_install = False
        for node, ssh_future in futures_ssh_installed.items():
            if not ssh_future.result():
                print('SSH keys not installed in node: {}'.format(node))
                do_install = True
        if do_install:
            internal_keypair = cluster_keypair
            if not internal_keypair:
                internal_keypair = _make_keypair()
            futures_ssh_install = {node: executor.submit(_install_ssh, wrapper.connection, ssh_module, reservation, internal_keypair, user, use_sudo=use_sudo) for node, wrapper in connectionwrappers.items()}
            state_ok = True
            for node, ssh_future in futures_ssh_install.items():
                if not ssh_future.result():
                    printe('Could not setup internal ssh key for node: {}'.format(node))
                    state_ok = False
            if local_connections:
                close_wrappers(connectionwrappers)
            return state_ok
        else:
            prints('SSH keys already installed.')
            if local_connections:
                close_wrappers(connectionwrappers)
            return True


def install(reservation, install_dir=defaults.install_dir(), key_path=None, admin_id=None, connectionwrapper=None, arrow_url=defaults.arrow_url(), use_sudo=defaults.use_sudo(), force_reinstall=False, debug=False, silent=False, cores=defaults.cores()):
    '''Installs RADOS-ceph on remote cluster.
    Warning: Requires that usernames on remote cluster nodes are equivalent.
    Warning: Requires passwordless communication between nodes on the local network. Use "install_ssh()" to accomplish this.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to install RADOS-Ceph on.
        install_dir (optional str): Location on remote host to compile RADOS-arrow in.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id that must become the admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        connectionwrapper (optional RemotoSSHWrapper): If set, uses given connection, instead of building a new one.
        arrow_url (optional str): Download URL for Arrow library to use with RADOS-Ceph.
        use_sudo (optional bool): If set, uses sudo during installation. Tries to avoid it otherwise.
        force_reinstall (optional bool): If set, we always will re-download and install Arrow. Otherwise, we will skip installing if we already have installed Arrow.
        debug (optional bool): If set, we compile Arrow using debug flags.
        silent (optional bool): If set, does not print so much info.
        cores (optional int): Number of cores to compile RADOS-arrow with.

    Returns:
        `True, admin_node_id` on success, `False, None` otherwise.'''
    if not _check_users(reservation):
        printe('Found different usernames between nodes. All nodes must have the same user login!')
        return False, None

    admin_picked, _ = _pick_admin(reservation, admin=admin_id)
    printc('Picked admin node: {}'.format(admin_picked), Color.CAN)

    local_connections = connectionwrapper == None

    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrapper = get_wrapper(admin_picked, admin_picked.ip_public, ssh_params=ssh_kwargs, silent=silent)
    else:
        if not connectionwrapper.open:
            raise ValueError('Cannot use already closed connection.')

    rados_module = _generate_module_rados()
    retval = _install_rados(connectionwrapper.connection, rados_module, reservation, install_dir, arrow_url=arrow_url, force_reinstall=force_reinstall, debug=debug, silent=silent, cores=cores), admin_picked.node_id

    if local_connections:
        close_wrappers([connectionwrapper])
    return retval