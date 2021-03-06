import subprocess
import concurrent.futures


def chown_key_conf(connection):
    '''Changes ownership of config and client keyring back to root. This is required to use RADOS without having to use sudo for everything.'''
    _, _, code = remoto.process.check(connection, 'sudo chown root:root /etc/ceph/ceph.conf', shell=True)
    if code != 0:
        return False
    _, _, code = remoto.process.check(connection, 'sudo chown root:root /etc/ceph/ceph.client.admin.keyring', shell=True)
    return code == 0


def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def stop_rados_memstore(reservation_str, mountpoint_path, silent):
    '''Stops a Ceph cluster.
    Args:
        reservation_str (str): String representation of a `metareserve.reservation.Reservation`. 
                               Nodes used for the Ceph cluster are expected to contain a 'designations' key in the `Node.extra_info` field.
                               The value must be a comma-separated string of lowercase `Designation` names, e.g. 'designations=osd,mon,mgr,mds'.
                               The specified daemons will be halted.
        mountpoint_path (str): Path to mount CephFS to on ALL nodes.
        silent (bool): If set, prints are less verbose.

    Returns:
        `True` on success, `False` otherwise.'''
    reservation = Reservation.from_string(reservation_str)

    ceph_nodes = [x for x in reservation.nodes if 'designations' in x.extra_info and any(x.extra_info['designations'])]
    monitors = [x for x in ceph_nodes if Designation.MON.name.lower() in x.extra_info['designations'].split(',')]
    managers = [x for x in ceph_nodes if Designation.MGR.name.lower() in x.extra_info['designations'].split(',')]
    mdss = [x for x in ceph_nodes if Designation.MDS.name.lower() in x.extra_info['designations'].split(',')]
    osds = [x for x in ceph_nodes if Designation.OSD.name.lower() in x.extra_info['designations'].split(',')]


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
        connectionwrappers = get_wrappers(reservation.nodes, lambda node: node.ip_local, ssh_params=lambda node: _merge_kwargs(ssh_kwargs, {'User': node.extra_info['user']}), silent=silent)

        if any(True for x in connectionwrappers.values() if not x):
            printe('Could not connect to some nodes.')
            close_wrappers(connectionwrappers)
            return False

        # Begin halting procedure
        futures_chown_files = [executor.submit(chown_key_conf, connectionwrappers[x].connection) for x in reservation.nodes]
        if not all(x.result for x in futures_chown_files):
            printe('Could not change ownerships back to root all nodes.')
            close_wrappers(connectionwrappers)
            return False

        if not silent:
            print('Unmounting CephFS mountpoints...')
        
        futures_stop_cephfs = [executor.submit(stop_cephfs, connectionwrappers[x].connection, mountpoint_path, silent) for x in reservation.nodes]
        for x in futures_stop_cephfs:
            x.result()

        if not silent:
            prints('Unmounted CephFS mountpoints')
            print('Stopping OSDs...')
        stop_osds_memstore(osds, silent) # OSDs are halted to ensure no side-effects occur when calling this function multiple times.
        if not silent:
            prints('Stopped OSDs')
            print('Stopping monitors...')
        
        futures_stop_monitors = [executor.submit(stop_monitor, x, connectionwrappers[x].connection, silent) for x in monitors]
        for x in futures_stop_monitors:
            x.result()

        if not silent:
            prints('Stopped monitors')
            print('Stopping managers...')

        futures_stop_managers = [executor.submit(stop_manager, x, connectionwrappers[x].connection, silent) for x in managers]
        for x in futures_stop_managers:
            x.result()

        if not silent:
            prints('Stopped managers')
            print('Stopping MDSs...')
        
        futures_stop_mdss = [executor.submit(stop_mds, x, connectionwrappers[x].connection, silent) for x in mdss]
        for x in futures_stop_mdss:
            x.result()

        if not silent:
            prints('Stopped old MDSs')
        close_wrappers(connectionwrappers)
        return True