import subprocess
import concurrent.futures

def get_subprocess_kwargs(silent):
    if silent:
        return {'shell': True, 'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL}
    return {'shell': True}


def send_config_keys(nodes, ceph_deploypath, silent):
    '''Pushes configuration and client.admin.key to given hosts.
    Args:
        nodes (iterable(str)): Iterable of hostnames to push config to.
        ceph_deploypath (str): Path to ceph-deploy binary.
        silent (bool): If set, prints less output.

    Returns:
        `True` on success, `False` otherwise.'''
    cmd = '{} --overwrite-conf admin {}'.format(ceph_deploypath, ' '.join(x.name for x in nodes))
    return subprocess.call(cmd, **get_subprocess_kwargs(silent)) == 0


def send_config(nodes, ceph_deploypath, silent):
    cmd = '{} --overwrite-conf config push {}'.format(ceph_deploypath, ' '.join(x.name for x in nodes))
    return subprocess.call(cmd, get_subprocess_kwargs(silent)) == 0


def update_config(nodes, ceph_deploypath, silent):
    '''Edit ceph.config and push it to all nodes. By default, the config is found in admin home directory.'''
    path = '{}/ceph.conf'.format(maindir())

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
        
    if fs.isfile(path):
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

    if not restart_monitors(monitors, silent):
        return False

    if subprocess.call('sudo cp {} /etc/ceph/ceph.conf'.format(path), get_subprocess_kwargs(silent)) != 0:
        return False
    return subprocess.call('sudo cp /users/{}/ceph.client.admin.keyring /etc/ceph/ceph.client.admin.keyring'.format(self.admin.user), get_subprocess_kwargs(silent)) == 0


def copy_osd_keys(self):
    '''Copies osd keyrings to OSDs.''' 
    executors = [Executor('scp ceph.bootstrap-osd.keyring {}:~/'.format(x.name), get_subprocess_kwargs(silent)) for x in self.osds]
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        printe('Could not scp keyring to all OSDs.')
        return False

    executors = [Executor('ssh {} "sudo cp ceph.bootstrap-osd.keyring /etc/ceph/ceph.keyring"'.format(x.name), shell=True, **self.call_opts()) for x in self.osds]
    executors += [Executor('ssh {} "sudo cp ceph.bootstrap-osd.keyring /var/lib/ceph/bootstrap-osd/ceph.keyring"'.format(x.name), shell=True, **self.call_opts()) for x in self.osds]
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        printe('Could not install keyring on all OSDs.')
        return False
    return True


def create_pools(self):
    '''Create ceph pools.'''
    try:
        subprocess.check_call('sudo ceph osd pool create cephfs_data 64', shell=True, **self.call_opts())
        subprocess.check_call('sudo ceph osd pool create cephfs_metadata 64', shell=True, **self.call_opts())
        subprocess.check_call('sudo ceph fs new cephfs cephfs_metadata cephfs_data', shell=True, **self.call_opts())
        subprocess.check_call('sudo mkdir -p /mnt/cephfs', shell=True, **self.call_opts())
        subprocess.check_call('sudo apt install ceph-fuse -y', shell=True, **self.call_opts())
        return True
    except Exception as e:
        printe('Experienced error: {}'.format(e))
        return False




def start_cephfs(self):
    '''Starts cephFS on /mnt/cephfs.
    Args:
        ceph_deploy: Path to `ceph-deploy` executable.
        mdss (iterable of `Info`): `Info` objects for nodes with metadata server designation. 

    Returns:
        `True` on success, `False` on failure.'''
    # Must re-create pools first
    if not self.create_pools():
        return False

    self.stop_cephfs()
    if Designation.OSD in self.admin.designations:
        if subprocess.call('sudo cp ceph.bootstrap-osd.keyring /var/lib/ceph/bootstrap-osd/ceph.keyring', shell=True) != 0:
            return False

    import time
    for y in range(60):
        if subprocess.call('sudo ceph-fuse /mnt/cephfs', shell=True, **self.call_opts()) == 0:
            prints('Succesfully called ceph-fuse (attempt {}/60)'.format(y+1))
            return True
        else:
            printw('Executing ceph-fuse... (attempt {}/60)'.format(y+1))
        time.sleep(1)
    return False


def generate_connections():
    pass


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

def start_rados(reservation_str, silent, retries):
    reservation = Reservation.from_string(reservation_str)

    monitors = [x for x in reservation.nodes if 'designations' in x.extra_info and Designation.MON.name.lower() in x.extra_info['designations'].split()]
    managers = [x for x in reservation.nodes if 'designations' in x.extra_info and Designation.MGR.name.lower() in x.extra_info['designations'].split()]
    mdss = [x for x in reservation.nodes if 'designations' in x.extra_info and Designation.MDS.name.lower() in x.extra_info['designations'].split()]
    osds = [x for x in reservation.nodes if 'designations' in x.extra_info and Designation.OSD.name.lower() in x.extra_info['designations'].split()]

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
        connectionwrappers = {x: val.result() for key, val in futures_connection.items()}

        if any(True for x in connectionwrappers.values() if not x):
            printe('Could not connect to some nodes.')
            return False

        # Begin starting procedure
        if not silent:
            print('Starting monitors...')
        if not (create_monitors(monitors, ceph_deploypath, silent) and start_monitors(ceph_deploypath, silent) and send_config_keys(set(monitors).union(set(managers)))):
            return False
        if not silent:
            prints('Started monitors')
            print('Stopping managers...')

        # stop_managers(managers, silent) # Managers are halted and recreated to ensure no side-effects occur when calling this function multiple times.
        futures_stop_managers = [executor.submit(stop_manager, connectionwrappers[x].connection, silent) for x in managers]
        if any(True for x in futures_stop_managers if not x.result()):
            return False

        if not silent:
            prints('Stopped managers')
            print('Starting managers...')
        if not start_managers(managers, ceph_deploypath, silent):
            return False
        if not silent:
            prints('Started managers')
            print('Editing configs...')
        if not update_config(nodeset, ceph_deploypath, silent):
            return False
        if not silent:
            prints('Edited configs')
            print('Deploying OSD keys...')
        if not copy_osd_keys():
            return False
        if not silent:
            prints('Deployed OSD keys')
            print('Stopping old OSD runs...')
        stop_osds() # OSDs are halted and recreated to ensure no side-effects occur when calling this function multiple times.
        if not silent:
            prints('Stopped old OSD runs')
            print('Booting OSDs...')
        if not start_osds():
            return False
        if not silent:
            prints('Booted OSDs')
            print('Stopping old mdss...')
        stop_mdss() # MDSs are halted and recreated to ensure no side-effects occur when calling this function multiple times.
        if not silent:
            prints('Stopped old mdss')
            print('Starting mdss...')
        if not start_mdss():
            return False
        if not silent:
            prints('Started mdss')
            print('Starting CephFS...')
        if not start_cephfs():
            return False
        if not silent:
            prints('Ceph mountpoints ready. Ceph cluster ready!')
        return True