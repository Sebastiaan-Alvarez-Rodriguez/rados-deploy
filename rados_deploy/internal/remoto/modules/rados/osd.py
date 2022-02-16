import concurrent.futures
import subprocess
import uuid

import remoto.process

'''Utility functions to control osds.
Requires:
    Executor (executor)
    rados_util'''

def stop_osds_memstore(osds, silent):
    '''Completely stops and removes all old running OSDs. Does not return anything.
    Warning: First, CephFS must be stopped, and secondly, the Ceph pools must removed, before calling this function.'''


    # stopping osds
    executors = [Executor('ssh {} "sudo systemctl stop ceph-osd.target"'.format(x.hostname), **get_subprocess_kwargs(silent)) for x in osds]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)

    # removing osds
    executors = [Executor('sudo ceph osd down osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)
    executors = [Executor('sudo ceph osd out osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)
    executors = [Executor('sudo ceph osd rm osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)

    # remove from crush
    executors = [Executor('sudo ceph osd crush rm osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)

    # remove from auth
    executors = [Executor('sudo ceph auth del osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds))]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)


def stop_osds_bluestore(osds, silent):
    # stopping osds
    executors = [Executor('ssh {} "sudo systemctl stop ceph-osd.target"'.format(x.hostname), **get_subprocess_kwargs(silent)) for x in osds]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=True)

     # removing osds
    executors = [Executor('sudo ceph osd down osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds)+20)]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)
    executors = [Executor('sudo ceph osd out osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds)+20)]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)
    executors = [Executor('sudo ceph osd rm osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds)+20)]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)

    # remove from crush
    executors = [Executor('sudo ceph osd crush rm osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds)+20)]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)

    # remove from auth
    executors = [Executor('sudo ceph auth del osd.{}'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in range(len(osds)+20)]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)

    # Stopping bluestore
    executors = [Executor('ssh {} "sudo ceph-volume lvm zap {} --destroy"'.format(x.hostname, x.extra_info['device_path']), **get_subprocess_kwargs(silent)) for x in osds]
    Executor.run_all(executors)
    Executor.wait_all(executors, stop_on_error=False, print_on_error=False)


def _remoto_check_call(connection, cmd, **kwargs):
    stdout, stderr, exitcode = remoto.process.check(connection, cmd, **kwargs)
    if exitcode == 0:
        return stdout, stderr, exitcode
    err = subprocess.CalledProcessError(exitcode, cmd)
    err.stderr = stderr
    err.stdout = stdout
    raise err


def start_osd_memstore(osd, connection, num_osds, silent):
    '''Starts a Ceph OSD manually, for memstore clusters.
    Args:
        osd (metareserve.Node): Node to start OSD daemon on.
        connection (remoto.Connection): Connection to given `osd`.
        num_osds (int): Amount of OSD daemons to spawn on local device.
        silent: If set, suppresses debug output.

    Returns:
        `True` on success, `False` on failure.'''
    def func(number, silent):
        new_uuid = uuid.uuid4()
        try:
            out, err, code = remoto.process.check(connection, 'sudo ceph-authtool --gen-print-key', shell=True)
            if code != 0:
                raise Exception(err)
            osd_secret = out[0].strip()

            out, err, code = remoto.process.check(connection, 'sudo ceph osd new {} -i - -n client.bootstrap-osd -k /var/lib/ceph/bootstrap-osd/ceph.keyring'.format(new_uuid), stdin='{{"cephx_secret": "{}"}}'.format(osd_secret).encode('utf-8'), shell=True)
            if code != 0:
                raise Exception(err)
            osd_id = out[0].strip()

            if not silent:
                print('[{}] Ceph secret: {}. UUID: {}. ID: {}'.format(number, osd_secret, new_uuid, osd_id))
            
            _, _, code = remoto.process.check(connection, 'sudo umount -f /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True)

            if code != 0 and code != 32: # We could not unmount and it was for another reason than the mountpoint not existing.
                return False
            _remoto_check_call(connection, 'sudo mkdir -p /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True)
            _remoto_check_call(connection, 'sudo rm -rf /var/lib/ceph/osd/ceph-{}/*'.format(osd_id), shell=True)
            _remoto_check_call(connection, 'sudo ceph-authtool --create-keyring /var/lib/ceph/osd/ceph-{0}/keyring --name osd.{0} --add-key {1}'.format(osd_id, osd_secret), shell=True)
            _remoto_check_call(connection, 'sudo ceph-osd -i {} --mkfs --osd-uuid {}'.format(osd_id, new_uuid), shell=True)
            _remoto_check_call(connection, 'sudo chown -R ceph:ceph /var/lib/ceph/osd/ceph-{}'.format(osd_id), shell=True)
            _remoto_check_call(connection, 'sudo systemctl enable ceph-osd@{}'.format(osd_id), shell=True)
            _remoto_check_call(connection, 'sudo systemctl start ceph-osd@{}'.format(osd_id), shell=True)
            return True
        except Exception as e:
            printe('[{}] Experienced error: {}'.format(number, e))
            return False
    with concurrent.futures.ThreadPoolExecutor(max_workers=num_osds) as executor:
        futures = [executor.submit(func, x, silent) for x in range(num_osds)]
        return all(x.result() for x in futures)


def start_osd_bluestore(ceph_deploypath, osd, num_osds, silent, use_ceph_volume):
    '''Starts a Ceph OSD for bluestore clusters.
    Requires that a key "device_path" is set in the extra_info of the node, which points to a device that will serve as data storage location.
    Args:
        ceph_deploypath (str): Absolute path to ceph_deploy.
        osd (metareserve.Node): Node to start OSD daemon on.
        num_osds (int): Amount of OSD daemons to spawn on local device.
        silent: If set, suppresses debug output.
        use_ceph_volume: If set, uses 'ceph-volume' instead of 'osd create'

    Returns:
        `True` on success, `False` on failure.'''
    if use_ceph_volume:
        executors = [Executor('ssh {} "sudo ceph-volume lvm batch --yes --no-auto --osds-per-device {} {}"'.format(osd.hostname, num_osds, osd.extra_info['device_path'].split(',')[x]), **get_subprocess_kwargs(silent)) for x in range(len(osd.extra_info['device_path'].split(',')))]
    else:
        executors = [Executor('{} -q osd create --data {} {}'.format(ceph_deploypath, osd.extra_info['device_path'].split(',')[x], osd.hostname), **get_subprocess_kwargs(silent)) for x in range(num_osds)]
    Executor.run_all(executors) 
    return Executor.wait_all(executors, print_on_error=True)


def restart_osds(osds, silent):
    '''Restarts managers. An essential feature for when you modify configs and need to reload for changes to take effect.'''
    executors = [Executor('ssh {} "sudo systemctl restart ceph-osd.target"'.format(x.hostname), **get_subprocess_kwargs(silent)) for x in osds]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)