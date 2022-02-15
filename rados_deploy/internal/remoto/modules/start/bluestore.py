import subprocess
import concurrent.futures


def update_config(nodes, ceph_deploypath, osd_op_threads, osd_pool_size, osd_max_obj_size, use_client_cache, silent):
    '''Edit ceph.config and push it to all nodes. By default, the config is found in admin home directory.
    Note: Afterwards, monitors must be restarted for the changes to take effect!
    Args:
        nodes (list(metareserve.Node): List of nodes to update config for.
        ceph_deploypath (str): Path to ceph_deploy executable.
        osd_op_threads (int): Number of op threads to use for each OSD. Make sure this number is not greater than the amount of cores each OSD has.
        osd_pool_size (int): Fragmentation of object to given number of OSDs. Must be less than or equal to amount of OSDs.
        osd_max_obj_size (int): Maximal object size in bytes. Normal=128*1024*1024 (128MB).
        use_client_cache (bool): If set, enables clients to cache data.
        silent (bool): If set, prints less output.

    Returns:
        `True` on success, `False` on failure.'''
    path = join(os.path.expanduser('~/'), 'ceph.conf')

    rules = {
        'fuse disable pagecache': 'false' if use_client_cache else 'true',
        'mon allow pool delete': 'true',
        'osd class load list': '*',
        'osd op threads': str(osd_op_threads),
        'osd pool default size': str(osd_pool_size),
        'osd_max_object_size': str(osd_max_obj_size),
    }

    import configparser
    parser = configparser.ConfigParser()

    if isfile(path):
        parser.optionxform=str
        parser.read(path)

        if not silent:
            print('Checking existing file ({}) for conflicting rules...'.format(path))

        found_type = determine_config_type(parser)
        if found_type != StorageType.BLUESTORE:
            printw('\tFound conflict: Current config is for "{}", but we deploy "{}". rebuilding config from scratch...'.format(found_type.name.lower(), StorageType.BLUESTORE.name.lower()))
            parser.remove_section('global')
        else:
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
    _, _, code = remoto.process.check(connection, 'sudo cp ceph.bootstrap-osd.keyring /etc/ceph/ceph.keyring', shell=True)
    if code != 0:
        return False

    _, _, code = remoto.process.check(connection, 'sudo cp ceph.bootstrap-osd.keyring /var/lib/ceph/bootstrap-osd/ceph.keyring', shell=True)
    return code == 0


def get_primary_group(connection):
    '''Fetches the primary group name of the current user and returns it.'''
    out, err, code = remoto.process.check(connection, 'id -gn', shell=True)
    if code != 0:
        return None
    return '\n'.join(out).strip()


def chown_key_conf(connection, user):
    '''Changes ownership of config and client keyring to user. This is required to use RADOS without having to use sudo for everything.'''
    groupname = get_primary_group(connection)
    if not groupname:
        return False

    _, _, code = remoto.process.check(connection, 'sudo chown {}:{} /etc/ceph/ceph.conf'.format(user, groupname), shell=True)
    if code != 0:
        return False
    _, _, code = remoto.process.check(connection, 'sudo chown {}:{} /etc/ceph/ceph.client.admin.keyring'.format(user, groupname), shell=True)
    return code == 0


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def start_rados_bluestore(reservation_str, mountpoint_path, osd_op_threads, osd_pool_size, osd_max_obj_size, placement_groups, use_client_cache, use_ceph_volume, silent, retries):
    '''Starts a Ceph cluster with RADOS-Arrow support.
    Args:
        reservation_str (str): String representation of a `metareserve.reservation.Reservation`. 
                               Nodes to use for the Ceph cluster are expected to contain a 'designations' key in the `Node.extra_info` field.
                               The value must be a comma-separated string of lowercase `Designation` names, e.g. 'designations=osd,mon,mgr,mds'.
                               Note: When a node specifies the 'osd' designation X times, that node will host X osds.
        mountpoint_path (str): Path to mount CephFS to on ALL nodes.
        osd_op_threads (int): Number of op threads to use for each OSD. Make sure this number is not greater than the amount of cores each OSD has.
        osd_pool_size (int): Fragmentation of object to given number of OSDs. Must be less than or equal to amount of OSDs.
        osd_max_obj_size (int): Maximal object size in bytes. Normal=128*1024*1024 (128MB).
        placement_groups (int): Amount of placement groups in Ceph.
        use_client_cache (bool): Toggles using cephFS I/O cache.
        use_ceph_volume (bool): If set, uses 'ceph-volume' instead of 'osd create'
        silent (bool): If set, prints are less verbose.
        retries (int): Number of retries for potentially failing operations.

    Returns:
        `True` on success, `False` on failure.'''
    reservation = Reservation.from_string(reservation_str)

    ceph_nodes = [x for x in reservation.nodes if 'designations' in x.extra_info and any(x.extra_info['designations'])]
    monitors = [x for x in ceph_nodes if Designation.MON.name.lower() in x.extra_info['designations'].split(',')]
    managers = [x for x in ceph_nodes if Designation.MGR.name.lower() in x.extra_info['designations'].split(',')]
    mdss = [x for x in ceph_nodes if Designation.MDS.name.lower() in x.extra_info['designations'].split(',')]
    osds = [x for x in ceph_nodes if Designation.OSD.name.lower() in x.extra_info['designations'].split(',')]

    if len(monitors) < 3:
        printe('We require at least 3 nodes with the "{}" designation (found {}).'.format(Designation.MON.name.lower(), len(monitors)))
        return False
    if len(managers) < 2:
        printe('We require at least 2 nodes with the "{}" designation (found {}).'.format(Designation.MGR.name.lower(), len(managers)))
        return False
    if len(mdss) < 2:
        printe('We require at least 2 nodes with the "{}" designation (found {}).'.format(Designation.MDS.name.lower(), len(mdss)))
        return False
    counted_total_osds = sum([sum(1 for y in x.extra_info['designations'].split(',') if y == Designation.OSD.name.lower()) for x in osds])
    if counted_total_osds < 3:
        printe('We require at least 3 nodes with the "{}" designation (found {}).'.format(Designation.OSD.name.lower(), counted_total_osds))
        return False
    

    ceph_deploypath = join(os.path.expanduser('~/'), '.local', 'bin', 'ceph-deploy')

    if not isfile(ceph_deploypath):
        printe('Could not find ceph-deploy at "{}". This is not the admin node, or you did not run the "install" command of this program.')
        return False

    keyfile = join(os.path.expanduser('~/'), '.ssh', 'rados_deploy.rsa')
    if not isfile(keyfile):
        printe('Could not find private key for internal cluster comms at "{}". Run the "install" command of this program.'.format(keyfile))
        return False

    with concurrent.futures.ThreadPoolExecutor(max_workers=len(reservation)) as executor:
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no', 'IdentityFile': keyfile}

        connectionwrappers = get_wrappers(reservation.nodes, lambda node: node.ip_public, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}), silent=silent)

        if any(True for x in connectionwrappers.values() if not x):
            printe('Could not connect to some nodes.')
            close_wrappers(connectionwrappers)
            return False

        # Begin starting procedure
        if not silent:
            print('Starting monitors...')
        if not (create_monitors(monitors, ceph_deploypath, silent) and start_monitors(ceph_deploypath, silent) and send_config_with_keys(set(monitors).union(set(managers)), ceph_deploypath, silent)):
            close_wrappers(connectionwrappers)
            return False
        if not silent:
            prints('Started monitors')
            print('Stopping managers...')

        # Managers are halted and recreated to ensure no side-effects occur when calling this function multiple times.
        futures_stop_managers = [executor.submit(stop_manager, x, connectionwrappers[x].connection, silent) for x in managers]
        for x in futures_stop_managers:
            x.result()

        if not silent:
            prints('Stopped managers')
            print('Starting managers...')
        if not start_managers(managers, ceph_deploypath, silent):
            close_wrappers(connectionwrappers)
            return False
        if not silent:
            prints('Started managers')
            print('Editing configs...')
        if not (update_config(ceph_nodes, ceph_deploypath, osd_op_threads, osd_pool_size, osd_max_obj_size, use_client_cache, silent) and restart_monitors(monitors, silent)):
            return False
        if not silent:
            prints('Edited configs')
            print('Deploying OSD keys...')
        if not copy_osd_keys(osds, silent):
            close_wrappers(connectionwrappers)
            return False

        futures_install_osd_keys = [executor.submit(install_osd_key, connectionwrappers[x].connection, silent) for x in osds]
        if not all(x.result() for x in futures_install_osd_keys):
            close_wrappers(connectionwrappers)
            return False

        if not silent:
            prints('Deployed OSD keys')
            print('Stopping old OSDs...')
        
        futures_stop_cephfs = [executor.submit(stop_cephfs, connectionwrappers[x].connection, mountpoint_path, silent) for x in reservation.nodes]
        for x in futures_stop_cephfs:
            x.result()

        destroy_pools(silent) # Must destroy old pools
        stop_osds_bluestore(osds, silent) # OSDs are halted to ensure no side-effects occur when calling this function multiple times.
        if not silent:
            prints('Stopped old OSDs')
            print('Booting OSDs...')

        futures_start_osds = []
        for x in osds:
            num_osds = len([1 for y in x.extra_info['designations'].split(',') if y == Designation.OSD.name.lower()])
            futures_start_osds.append(executor.submit(start_osd_bluestore, ceph_deploypath, x, num_osds, silent, use_ceph_volume))
        if not all(x.result() for x in futures_start_osds):
            close_wrappers(connectionwrappers)
            return False

        if not silent:
            prints('Booted OSDs')
            print('Stopping old MDSs...')
        
        futures_stop_mdss = [executor.submit(stop_mds, x, connectionwrappers[x].connection, silent) for x in mdss]
        for x in futures_stop_mdss:
            x.result()

        if not silent:
            prints('Stopped old MDSs')
            print('Starting mdss...')
        if not start_mdss(mdss, ceph_deploypath, silent):
            close_wrappers(connectionwrappers)
            return False
        if not silent:
            prints('Started MDSs')
            print('Starting CephFS...')
        if not create_pools(placement_groups, silent):
            close_wrappers(connectionwrappers)
            return False

        futures_stop_cephfs = [executor.submit(stop_cephfs, connectionwrappers[x].connection, mountpoint_path, silent) for x in reservation.nodes]
        for x in futures_stop_cephfs:
            x.result()

        futures_start_cephfs = [executor.submit(start_cephfs, x, connectionwrappers[x].connection, ceph_deploypath, path=mountpoint_path, use_client_cache=use_client_cache, retries=retries, silent=silent) for x in reservation.nodes]    
        if not all(x.result() for x in futures_start_cephfs):
            printe('Not all nodes could setup mountpoints.')
            close_wrappers(connectionwrappers)
            return False

        futures_chown_files = [executor.submit(chown_key_conf, connectionwrapper.connection, node.extra_info['user']) for node, connectionwrapper in connectionwrappers.items()]
        if not all(x.result() for x in futures_chown_files):
            printe('Could not chown ceph.conf and client keyring on every node')
            close_wrappers(connectionwrappers)
            return False

        if not silent:
            prints('Ceph cluster ready!')
        close_wrappers(connectionwrappers)
        return True