import subprocess


def create_monitors(self):
    '''Creates new monitor nodes.'''
    cmd = '{} new {}'.format(self.ceph_deploypath, ' '.join(x.name for x in self.monitors))
    return subprocess.call(cmd, shell=True, **self.call_opts()) == 0


def start_monitors(self):
    '''Start monitor nodes.'''
    cmd = '{} --overwrite-conf mon create-initial'.format(self.ceph_deploypath)
    if not subprocess.call(cmd, shell=True, **self.call_opts()) == 0:
        return False
    cmd = '{} --overwrite-conf admin {}'.format(self.ceph_deploypath, ' '.join(x.name for x in set(self.monitors).union(set(self.managers))))
    return subprocess.call(cmd, shell=True, **self.call_opts()) == 0


def start_managers(self):
    '''Create and start manager nodes.'''
    cmd = '{} --overwrite-conf mgr create {}'.format(self.ceph_deploypath, ' '.join(x.name for x in self.managers))
    return subprocess.call(cmd, shell=True, **self.call_opts()) == 0


def update_config(self):
    '''Edit ceph.config and push it to all nodes. By default, the config is found in admin home directory.'''
    path = '/users/{}/ceph.conf'.format(self.admin.user)

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

        print('Checking existing file ({}) for conflicting rules...'.format(path))
        
        for key in parser['global']:
            if key in rules and parser['global'][key] != rules[key]: # Rule is present in current file, with incorrect value
                printw('\tFound conflict: rule={}, found val={}, new val={}'.format(key, parser['global'][key], rules[key]))

    for key in rules:
        parser['global'][key] = rules[key]

    with open(path, 'w') as file:
        parser.write(file)

    cmd = '{} --overwrite-conf config push {}'.format(self.ceph_deploypath, ' '.join(x.name for x in self.nodes))
    if subprocess.call(cmd, shell=True, **self.call_opts()) != 0:
        return False

    executors = [Executor('ssh {} "sudo systemctl restart ceph-mon.target"'.format(x.name), shell=True, **self.call_opts()) for x in self.monitors] # Restart monitors, as their config has been updated and they already run.
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        return False

    if subprocess.call('sudo cp {} /etc/ceph/ceph.conf'.format(path), shell=True, **self.call_opts()) != 0:
        return False
    return subprocess.call('sudo cp /users/{}/ceph.client.admin.keyring /etc/ceph/ceph.client.admin.keyring'.format(self.admin.user), shell=True, **self.call_opts()) == 0


def copy_osd_keys(self):
    '''Copies osd keyrings to OSDs.''' 
    executors = [Executor('scp ceph.bootstrap-osd.keyring {}:~/'.format(x.name), shell=True, **self.call_opts()) for x in self.osds]
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


def start_osds(self):
    '''Boots OSDs. Because of the large amount of remote commands to be executed, continues framework execution on remote hosts in parallel.'''
    executors = [Executor('ssh {} "python3 MetaSpark/main.py ceph deploy cluster local {} --local-osd -st {} --num-osds {}"'.format(x.name, AllocationMethod.GENI_DIRECT.name, self.storetype.name, len([1 for y in x.designations if y == Designation.OSD])), shell=True, **self.call_opts()) for x in self.osds]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)
    # for x in self.osds:
    #     cmd = 'ssh {} "python3 MetaSpark/main.py ceph deploy cluster local {} --local-osd -st {} --num-osds {}"'.format(x.name, AllocationMethod.GENI_DIRECT.name, self.storetype.name, len([1 for y in x.designations if y == Designation.OSD]))
    #     if subprocess.call(cmd, shell=True) != 0:
    #         return False
    # return True


@staticmethod
def deploy_ceph_osd_local(amount, storetype=StoreType.MEMSTORE, debug=True):
    '''Starts a Ceph OSD. Must be called from the OSD node itself.
    Args:
        amount: Amount of OSD daemons to spawn on local device.
        storetype (`str` or `StoreType`): Determines the OSD storage mode. 
        debug: If set, we print a lot of subprocess command output. Subprocess commands are silent otherwise (default=`False`).

    Returns:
        `True` on success, `False` on failure.'''
    if storetype == StoreType.MEMSTORE:
        def func(debug, number):
            new_uuid = uuid.uuid4()
            try:
                osd_secret = subprocess.check_output('sudo ceph-authtool --gen-print-key', shell=True).decode('utf-8').strip()
                osd_id = subprocess.check_output('sudo ceph osd new {} -i - -n client.bootstrap-osd -k /var/lib/ceph/bootstrap-osd/ceph.keyring'.format(new_uuid), input='{{"cephx_secret": "{}"}}'.format(osd_secret).encode('utf-8'),  shell=True).decode('utf-8').strip()
                print('[{}] Ceph secret: {}. UUID: {}. ID: {}'.format(number, osd_secret, new_uuid, osd_id))

                umount_code = subprocess.call('sudo umount -f /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
                if umount_code != 0 and umount_code != 32: # We could not unmount and it was for another reason than the mountpoint not existing.
                    return False
                subprocess.check_call('sudo mkdir -p /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
                subprocess.check_call('sudo rm -rf /var/lib/ceph/osd/ceph-{}/*'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
                subprocess.check_call('sudo ceph-authtool --create-keyring /var/lib/ceph/osd/ceph-{0}/keyring --name osd.{0} --add-key {1}'.format(osd_id, osd_secret), shell=True, **CephDeployGENI.static_call_opts(debug))
                subprocess.check_call('sudo ceph-osd -i {} --mkfs --osd-uuid {}'.format(osd_id, new_uuid), shell=True, **CephDeployGENI.static_call_opts(debug))
                subprocess.check_call('sudo chown -R ceph:ceph /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
                subprocess.check_call('sudo systemctl enable ceph-osd@{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
                subprocess.check_call('sudo systemctl start ceph-osd@{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
                return True
            except Exception as e:
                printe('[{}] Experienced error: {}'.format(number, e))
                return False
        with concurrent.futures.ThreadPoolExecutor() as executor:
            futures = [executor.submit(func, debug, x) for x in range(amount)]
            return_values = [x.result() for x in futures]
        return all(return_values)
    else:
        raise NotImplementedError


def start_mdss(self):
    '''Starts metadata servers.'''
    cmd = '{} mds create {}'.format(self.ceph_deploypath, ' '.join(x.name for x in self.mdss))
    return subprocess.call(cmd, shell=True, **self.call_opts()) == 0


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
