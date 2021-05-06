import subprocess


'''Utility functions to control osds.
Requires:
    Executor (executor)
    rados_util'''

def stop_osds(osds, silent):
    '''Completely stops and removes all old running OSDs. Does not return anything.
    Warning: First, CephFS must be stopped, and seconfly, the Ceph pools must removed, before calling this function.'''

    # Must remove any old running cephfs first
    stop_cephfs()
    # Must remove old pools
    remove_pools()
    # stopping osds
    executors = [Executor('ssh {} "sudo systemctl stop ceph-osd.target"'.format(x.name), get_subprocess_kwargs(silent)) for x in osds]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)
    
    # removing osds
    executors = [Executor('sudo ceph osd down osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)
    executors = [Executor('sudo ceph osd out osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)
    executors = [Executor('sudo ceph osd rm osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)

    # remove from crush
    executors = [Executor('sudo ceph osd crush rm osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)

    # remove from auth
    executors = [Executor('sudo ceph auth del osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)


def start_osds(osds, silent):
    '''Boots OSDs. Because of the large amount of remote commands to be executed, continues framework execution on remote hosts in parallel.'''
    raise NotImplementedError('OSD creation time!')
    # TODO: Below code is executed to forward execution to each OSD node.
    # executors = [Executor('ssh {} "python3 MetaSpark/main.py ceph deploy cluster local {} --local-osd -st {} --num-osds {}"'.format(x.name, AllocationMethod.GENI_DIRECT.name, storetype.name, len([1 for y in x.designations if y == Designation.OSD])), shell=True, **call_opts()) for x in osds]
    # Executor.run_all(executors)
    # return Executor.wait_all(executors, print_on_error=True)

    # TODO: Below code is executed on each OSD node. Idea: Use and ship remoto? Probably impossible. However, we must install ceph-deploy already, and that has remoto...?
    # @staticmethod
    # def deploy_ceph_osd_local(amount, storetype=StoreType.MEMSTORE, debug=True):
    #     '''Starts a Ceph OSD. Must be called from the OSD node itself.
    #     Args:
    #         amount: Amount of OSD daemons to spawn on local device.
    #         storetype (`str` or `StoreType`): Determines the OSD storage mode. 
    #         debug: If set, we print a lot of subprocess command output. Subprocess commands are silent otherwise (default=`False`).

    #     Returns:
    #         `True` on success, `False` on failure.'''
    #     if storetype == StoreType.MEMSTORE:
    #         def func(debug, number):
    #             new_uuid = uuid.uuid4()
    #             try:
    #                 osd_secret = subprocess.check_output('sudo ceph-authtool --gen-print-key', shell=True).decode('utf-8').strip()
    #                 osd_id = subprocess.check_output('sudo ceph osd new {} -i - -n client.bootstrap-osd -k /var/lib/ceph/bootstrap-osd/ceph.keyring'.format(new_uuid), input='{{"cephx_secret": "{}"}}'.format(osd_secret).encode('utf-8'),  shell=True).decode('utf-8').strip()
    #                 print('[{}] Ceph secret: {}. UUID: {}. ID: {}'.format(number, osd_secret, new_uuid, osd_id))

    #                 umount_code = subprocess.call('sudo umount -f /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL)
    #                 if umount_code != 0 and umount_code != 32: # We could not unmount and it was for another reason than the mountpoint not existing.
    #                     return False
    #                 subprocess.check_call('sudo mkdir -p /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
    #                 subprocess.check_call('sudo rm -rf /var/lib/ceph/osd/ceph-{}/*'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
    #                 subprocess.check_call('sudo ceph-authtool --create-keyring /var/lib/ceph/osd/ceph-{0}/keyring --name osd.{0} --add-key {1}'.format(osd_id, osd_secret), shell=True, **CephDeployGENI.static_call_opts(debug))
    #                 subprocess.check_call('sudo ceph-osd -i {} --mkfs --osd-uuid {}'.format(osd_id, new_uuid), shell=True, **CephDeployGENI.static_call_opts(debug))
    #                 subprocess.check_call('sudo chown -R ceph:ceph /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
    #                 subprocess.check_call('sudo systemctl enable ceph-osd@{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
    #                 subprocess.check_call('sudo systemctl start ceph-osd@{}'.format(osd_id), shell=True, **CephDeployGENI.static_call_opts(debug))
    #                 return True
    #             except Exception as e:
    #                 printe('[{}] Experienced error: {}'.format(number, e))
    #                 return False
    #         with concurrent.futures.ThreadPoolExecutor() as executor:
    #             futures = [executor.submit(func, debug, x) for x in range(amount)]
    #             return_values = [x.result() for x in futures]
    #         return all(return_values)
    #     else:
    #         raise NotImplementedError


def restart_osds(osds, silent):
    '''Restarts managers. An essential feature for when you modify configs and need to reload for changes to take effect.'''
    executors = [Executor('ssh {} "sudo systemctl restart ceph-osd.target"'.format(x.name), **get_subprocess_kwargs(silent)) for x in osds]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)