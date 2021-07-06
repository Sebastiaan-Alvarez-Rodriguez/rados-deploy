from rados_deploy.internal.remoto.modulegenerator import ModuleGenerator
from rados_deploy.internal.remoto.ssh_wrapper import get_wrapper, get_wrappers, close_wrappers


def _generate_module_remote_install(silent=False):
    generation_loc = fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'generated', 'remote_install.py')
    files = [
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'util', 'printer.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'printer.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'util', 'importer.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'package_install.py'),
        fs.join(fs.dirname(fs.dirname(fs.abspath(__file__))), 'internal', 'remoto', 'modules', 'remoto_base.py'),
    ]
    ModuleGenerator().with_files(*files).generate(generation_loc, silent=silent)
    return importer.import_full_path(generation_loc)


def remote_pip_install(connection, module, name, usermode=True, py='python3', pip='pip3', silent=True):
    remote_module = connection.import_module(module)
    return remote_module.remote_pip_install(name, usermode, py, pip, silent)


def install(reservation_or_nodes, key_path=None, connectionwrappers=None, name=None, usermode=True, py='python3', pip='pip3', silent=True):
    if isinstance(reservation_or_nodes, metareserve.Reservation):
        if not reservation_or_nodes or len(reservation_or_nodes) == 0:
            raise ValueError('Reservation does not contain any items'+(' (reservation=None)' if not reservation else ''))
        nodes = reservation_or_nodes.nodes()
    else:
        nodes = list(reservation_or_nodes) if not isinstance(reservation_or_nodes, list) else reservation_or_nodes
        if not any(nodes):
            raise ValueError('No nodes provided.')

    local_connections = connectionwrapper == None

    if local_connections:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'User': nodes[0].extra_info['user'], 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path
       connectionwrappers = get_wrappers(nodes, lambda node: node.ip_public, ssh_params=ssh_kwargs, silent=silent)
    else:
        if not all(x.open for x in connectionwrappers):
            raise ValueError('Remote library installation failed: At least one connection is already closed.')

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(nodes)) as executor:
        remote_install_module = _generate_module_remote_install(silent=silent)

        futures_remote_install = {node: executor.submit(remote_pip_install, wrapper.connection, remote_install_module, name, usermode=usermode, py=py, pip=pip, silent=silent) for node, wrapper in connectionwrappers.items()}
        state_ok = True
        for node, future_remote_install in futures_remote_install.items():
            if not future_remote_install.result():
                print('Library installation failure in node: {}'.format(node))
                state_ok = False
    return state_ok