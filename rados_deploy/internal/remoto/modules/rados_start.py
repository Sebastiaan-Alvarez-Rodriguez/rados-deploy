import subprocess
import concurrent.futures

def get_subprocess_kwargs(silent):
    if silent:
        return {'shell': True, 'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL}
    return {'shell': True}


def update_config(nodes, ceph_deploypath, silent):
    '''Edit ceph.config and push it to all nodes. By default, the config is found in admin home directory.
    Note: Afterwards, monitors must be restarted for the changes to take effect!'''
    path = join(os.path.expanduser('~/'), 'ceph.conf')

    rules = {
        'mon allow pool delete': 'true',
        'osd objectstore': 'memstore',
        'osd class load list': '*',
        'memstore device bytes': str(10*1024*1024*1024),
        'osd op threads': str(4),
        'osd pool default size': str(3)
    }

    import configparser
    parser = configparser.ConfigParser()

    if isfile(path):
        parser.optionxform=str
        parser.read(path)

        if not silent:
            print('Checking existing file ({}) for conflicting rules...'.format(path))

        for key in parser['global']:
            if key in rules and parser['global'][key] != rules[key]: # Rule is present in current file, with incorrect value
                printw('\tFound conflict: rule={}, found val={}, new val={}'.format(key, parser['global'][key], rules[key]))

    for key in rules:
        parser['global'][key] = rules[key]

    with open(path, 'w') as file:
        parser.write(file)

    if not send_config(nodes, ceph_deploypath, silent):
        return False

    if subprocess.call('sudo mkdir -p /etc/ceph/', **get_subprocess_kwargs(silent)) != 0:
        return False
    if subprocess.call('sudo cp {} /etc/ceph/ceph.conf'.format(path), **get_subprocess_kwargs(silent)) != 0:
        return False
    return subprocess.call('sudo cp {} {}'.format(join(os.path.expanduser('~/'), 'ceph.client.admin.keyring'), '/etc/ceph/ceph.client.admin.keyring'), **get_subprocess_kwargs(silent)) == 0


def copy_osd_keys(osds, silent):
    '''Copies osd keyrings from admin homedir to each OSD homedir.''' 
    executors = [Executor('scp ~/ceph.bootstrap-osd.keyring {}:~/'.format(x.hostname), **get_subprocess_kwargs(silent)) for x in osds]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)


def install_osd_key(connection, silent):
    '''Installs an OSD key on a (!)single(!) osd.'''
    out, err, code = remoto.process.check(connection, 'sudo cp ceph.bootstrap-osd.keyring /etc/ceph/ceph.keyring', shell=True)
    if code != 0:
        return False

    out, err, code = remoto.process.check(connection, 'sudo cp ceph.bootstrap-osd.keyring /var/lib/ceph/bootstrap-osd/ceph.keyring', shell=True)
    return code == 0


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


'''
IDEA-0: make code available here to open nice remoto connections to all nodes. Can reuse connections every time we need to execute remotely on e.g. a manager node.
Note that we cannot use the module import feature here.
Need non-trivial present:
    0. remoto - check, available because ceph-deploy is already installed.

Need own sources:
    0. thirdparty.sshconf.sshconf (clean)
    1. internal.remoto.ssh_wrapper (clean)
    2. internal.remoto.util (clean, has non-trivial imports, disable warning when generating)
    3. metareserve's reservation.py
Can access obtaining connections in the 'regular' way after that.
'''

def start_rados(reservation_str, mountpoint_path, silent, retries):
    '''Starts a Ceph cluster with RADOS-Arrow support.
    Args:
        reservation_str (str): String representation of a `metareserve.reservation.Reservation`. 
                               Nodes to use for the Ceph cluster are expected to contain a 'designations' key in the `Node.extra_info` field.
                               The value must be a comma-separated string of lowercase `Designation` names, e.g. 'designations=osd,mon,mgr,mds'.
                               Note: When a node specifies the 'osd' designation X times, that node will host X osds.
        mountpoint_path (str): Path to mount CephFS to on ALL nodes.
        silent (bool): If set, prints are less verbose.
        retries (int): Number of retries for potentially failing operations
    '''
    reservation = Reservation.from_string(reservation_str)

    ceph_nodes = [x for x in reservation.nodes if 'designations' in x.extra_info and any(x.extra_info['designations'])]
    monitors = [x for x in ceph_nodes if Designation.MON.name.lower() in x.extra_info['designations'].split(',')]
    managers = [x for x in ceph_nodes if Designation.MGR.name.lower() in x.extra_info['designations'].split(',')]
    mdss = [x for x in ceph_nodes if Designation.MDS.name.lower() in x.extra_info['designations'].split(',')]
    osds = [x for x in ceph_nodes if Designation.OSD.name.lower() in x.extra_info['designations'].split(',')]

    if len(monitors) < 3:
        raise ValueError('We require at least 3 nodes with the "{}" designation.'.format(Designation.MON.name.lower()))
    if len(managers) < 2:
        raise ValueError('We require at least 2 nodes with the "{}" designation.'.format(Designation.MGR.name.lower()))
    if len(mdss) < 2:
        raise ValueError('We require at least 2 nodes with the "{}" designation.'.format(Designation.MDS.name.lower()))
    if len(osds) < 3:
        raise ValueError('We require at least 3 nodes with the "{}" designation.'.format(Designation.OSD.name.lower()))
    

    ceph_deploypath = join(os.path.expanduser('~/'), '.local', 'bin', 'ceph-deploy')

    if not isfile(ceph_deploypath):
        printe('Could not find ceph-deploy at "{}". Run the "install" command of this program, and be sure to pick the same admin id when doing that vs here.')
        return False

    keyfile = join(os.path.expanduser('~/'), '.ssh', 'rados_deploy.rsa')
    if not isfile(keyfile):
        printe('Could not find private key for internal cluster comms at "{}". Run the "install" command of this program.'.format(keyfile))
        return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no', 'IdentityFile': keyfile}
        futures_connection = {x: executor.submit(get_ssh_connection, x.ip_public, silent=silent, ssh_params=_merge_kwargs(ssh_kwargs, {'User': x.extra_info['user']})) for x in reservation.nodes}
        connectionwrappers = {key: val.result() for key, val in futures_connection.items()}

        if any(True for x in connectionwrappers.values() if not x):
            printe('Could not connect to some nodes.')
            return False

        # Begin starting procedure
        if not silent:
            print('Starting monitors...')
        if not (create_monitors(monitors, ceph_deploypath, silent) and start_monitors(ceph_deploypath, silent) and send_config_with_keys(set(monitors).union(set(managers)), ceph_deploypath, silent)):
            return False
        if not silent:
            prints('Started monitors')
            print('Stopping managers...')

        # Managers are halted and recreated to ensure no side-effects occur when calling this function multiple times.
        futures_stop_managers = [executor.submit(stop_manager, x, connectionwrappers[x].connection, silent) for x in managers]
        if not all(x.result() for x in futures_stop_managers):
            return False

        if not silent:
            prints('Stopped managers')
            print('Starting managers...')
        if not start_managers(managers, ceph_deploypath, silent):
            return False
        if not silent:
            prints('Started managers')
            print('Editing configs...')
        if not (update_config(ceph_nodes, ceph_deploypath, silent) and restart_monitors(monitors, silent)):
            return False
        if not silent:
            prints('Edited configs')
            print('Deploying OSD keys...')
        if not copy_osd_keys(osds, silent):
            return False

        futures_install_osd_keys = [executor.submit(install_osd_key, connectionwrappers[x].connection, silent) for x in osds]
        if not all(x.result() for x in futures_install_osd_keys):
            return False

        if not silent:
            prints('Deployed OSD keys')
            print('Stopping old OSDs...')
        
        futures_stop_cephfs = [executor.submit(stop_cephfs, connectionwrappers[x].connection, mountpoint_path, silent) for x in reservation.nodes]
        for x in futures_stop_cephfs:
            x.result()

        destroy_pools(silent) # Must destroy old pools
        stop_osds(osds, silent) # OSDs are halted to ensure no side-effects occur when calling this function multiple times.
        if not silent:
            prints('Stopped old OSDs')
            print('Booting OSDs...')

        futures_start_osds = []
        for x in osds:
            num_osds = len([1 for y in x.extra_info['designations'].split(',') if y == Designation.MON.name.lower()])
            futures_start_osds.append(executor.submit(start_osd, x, connectionwrappers[x].connection, num_osds, silent))
        if not all(x.result() for x in futures_start_osds):
            return False

        if not silent:
            prints('Booted OSDs')
            print('Stopping old MDSs...')
        
        futures_stop_mdss = [executor.submit(stop_mds, x, connectionwrappers[x].connection, silent) for x in mdss]
        if not all(x.result() for x in futures_stop_mdss):
            return False

        if not silent:
            prints('Stopped old MDSs')
            print('Starting mdss...')
        if not start_mdss(mdss, ceph_deploypath, silent):
            return False
        if not silent:
            prints('Started MDSs')
            print('Starting CephFS...')
        if not create_pools(silent):
            return False

        futures_stop_cephfs = [executor.submit(stop_cephfs, connectionwrappers[x].connection, mountpoint_path, silent) for x in reservation.nodes]
        for x in futures_stop_cephfs:
            x.result()

        futures_start_cephfs = [executor.submit(start_cephfs, x, connectionwrappers[x].connection, ceph_deploypath, mountpoint_path, retries, silent) for x in reservation.nodes]    
        if all(x.result() for x in futures_start_cephfs):
            if not silent:
                prints('Ceph mountpoints ready. Ceph cluster ready!')
            return True
        return False
        # # TODO: Stop and mount ceph using cephfs on all nodes.
        # stop_cephfs(mountpoint_path, silent)
        # if not start_cephfs(mountpoint_path, retries, silent):
        #     return False