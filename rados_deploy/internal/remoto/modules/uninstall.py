import subprocess

def uninstall(install_dir, silent):
    if subprocess.call('sudo apt purge ceph-base ceph-common ceph-fuse ceph-mgr ceph-osd ceph-mon librdmacm1 -y && sudo apt autoremove -y', **get_subprocess_kwargs(silent)) != 0:
        return False

    try:
        output = subprocess.check_output('sudo ls -l /var/lib/ceph/osd/ceph-*', **get_subprocess_kwargs(silent)).decode('utf-8')
        for x in output.split('\n'):
            location = x.split(' ')[-1]
            if subprocess.call('sudo umount /var/lib/ceph/osd/{}'.format(location), **get_subprocess_kwargs(silent)) != 0:
                return False    
    except subprocess.CalledProcessError as e: # There is no /var/lib/ceph/osd/.
        pass

    if subprocess.call('sudo rm -rf /var/lib/ceph', **get_subprocess_kwargs(silent)) != 0:
        return False

    if install_dir != None:
        return subprocess.call('sudo rm -rf {}'.format(install_dir), **get_subprocess_kwargs(silent)) == 0
    return True