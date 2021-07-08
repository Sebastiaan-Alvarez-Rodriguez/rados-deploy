import concurrent.futures

import rados_deploy.internal.defaults.install as install_defaults
from rados_deploy.internal.remoto.modulegenerator import ModuleGenerator
from rados_deploy.internal.remoto.ssh_wrapper import get_wrappers, close_wrappers
import rados_deploy.internal.util.fs as fs
import rados_deploy.internal.util.importer as importer
import rados_deploy.internal.util.location as loc
from rados_deploy.internal.util.printer import *


def _uninstall(connection, module, install_dir=None, silent=False):
    remote_module = connection.import_module(module)
    return remote_module.uninstall(install_dir, silent)


def _generate_module_uninstall(silent=False):
    '''Generates uninstall module from available sources.'''
    generation_loc = fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'generated', 'uninstall.py')
    files = [
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'util.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'uninstall.py'),
        fs.join(fs.dirname(fs.abspath(__file__)), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_module(fs).with_files(*files).generate(generation_loc, silent)
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


def uninstall(reservation, install_dir=install_defaults.install_dir(), key_path=None, admin_id=None, connectionwrappers=None, silent=False):
    '''Uninstalls RADOS-ceph on remote cluster Assumes that the system has been stopped already.
    Args:
        reservation (`metareserve.Reservation`): Reservation object with all nodes to install RADOS-Ceph on.
        install_dir (optional str): If set to location on remote host where we compiled RADOS-arrow in, removes that location.
        key_path (optional str): Path to SSH key, which we use to connect to nodes. If `None`, we do not authenticate using an IdentityFile.
        admin_id (optional int): Node id that must become the admin. If `None`, the node with lowest public ip value (string comparison) will be picked.
        connectionwrappers (optional dict(metareserve.Node, RemotoSSHWrapper)): If set, uses given connections, instead of building new ones.
        silent (optional bool): If set, does not print so much info.

    Returns:
        `True` on success, `False` otherwise.'''
    admin_picked, _ = _pick_admin(reservation, admin=admin_id)
    printc('Picked admin node: {}'.format(admin_picked), Color.CAN)

    local_connections = connectionwrappers == None

    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': admin_picked.extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
        connectionwrappers = get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=ssh_kwargs, silent=silent)
    else:
        if not all(x.open for x in connectionwrappers):
            raise ValueError('SSH installation failed: At least one connection is already closed.')

    uninstall_module = _generate_module_uninstall(silent=silent)

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        futures_uninstall = {node: executor.submit(_uninstall, wrapper.connection, uninstall_module, install_dir=install_dir, silent=silent) for node, wrapper in connectionwrappers.items()}
        state_ok = True
        for node, future_uninstall in futures_uninstall.items():
            if not future_uninstall.result():
                print('Could not uninstall RADOS-Ceph-Arrow deployment from node: {}'.format(node))
                state_ok = False
    if local_connections:
        close_wrappers(connectionwrappers)
    if state_ok:
        prints('Uninstalled RADOS-Ceph.')
        return True
    else:
        printe('There were problems while uninstalling RADOS-Ceph.')
        return False
