def stop_monitors(self):
    '''Stops monitors.'''
    # cmd = '{} mon destroy {}'.format(self.ceph_deploypath, ' '.join(x.name for x in self.monitors))
    # subprocess.call(cmd, shell=True, **self.call_opts())
    executors = [Executor('ssh {} "sudo systemctl stop ceph-mon.target"'.format(x.name), shell=True, **self.call_opts()) for x in self.monitors]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)


def stop_managers(self):
    '''Stops managers.'''
    executors = [Executor('ssh {} "sudo systemctl stop ceph-mgr.target"'.format(x.name), shell=True, **self.call_opts()) for x in self.managers]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)


def remove_pools(self):
    '''Removes all knowledge of the existing pools/data.'''
    # delete a filesystem
    subprocess.call('sudo ceph fs fail cephfs', shell=True)
    subprocess.call('sudo ceph fs rm cephfs --yes-i-really-mean-it', shell=True)
    # delete the cephfs pools
    subprocess.call('sudo ceph osd pool rm cephfs_data cephfs_data --yes-i-really-really-mean-it', shell=True)
    subprocess.call('sudo ceph osd pool rm cephfs_metadata cephfs_metadata --yes-i-really-really-mean-it', shell=True)
    subprocess.call('sudo ceph osd pool rm device_health_metrics device_health_metrics --yes-i-really-really-mean-it', shell=True)


def stop_osds(self):
    '''Completely stops and removes all old running OSDs.'''
    # Must remove any old running cephfs first
    self.stop_cephfs()
    # Must remove old pools
    self.remove_pools()
    # stopping osds
    executors = [Executor('ssh {} "sudo systemctl stop ceph-osd.target"'.format(x.name), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in self.osds]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)
    
    # removing osds
    executors = [Executor('sudo ceph osd down osd.{0}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(self.osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)
    executors = [Executor('sudo ceph osd out osd.{0}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(self.osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)
    executors = [Executor('sudo ceph osd rm osd.{0}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(self.osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)

    # remove from crush
    executors = [Executor('sudo ceph osd crush rm osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(self.osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)

    # remove from auth
    executors = [Executor('sudo ceph auth del osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(self.osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)
    return True


def stop_mdss(self):
    '''Stops metadata servers.'''
    executors = [Executor('ssh {} "sudo systemctl stop ceph-mds.target"'.format(x.name), shell=True, **self.call_opts()) for x in self.mdss]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)


def stop_cephfs(self):
    '''Stops cephfs.'''
    subprocess.call('sudo fusermount -uz /mnt/cephfs', shell=True, **self.call_opts())


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))