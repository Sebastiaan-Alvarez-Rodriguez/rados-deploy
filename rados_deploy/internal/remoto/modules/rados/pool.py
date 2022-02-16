import subprocess

'''Utility functions to create and destroy ceph pools.
Requires:
    rados_util'''
def destroy_pools(silent):
        '''Removes all knowledge of the existing pools/data. Should be executed on the admin node.'''
        # delete a filesystem
        subprocess.call('sudo ceph fs fail cephfs', **get_subprocess_kwargs(silent))
        subprocess.call('sudo ceph fs rm cephfs --yes-i-really-mean-it', **get_subprocess_kwargs(silent))
        # delete the cephfs pools
        subprocess.call('sudo ceph osd pool rm cephfs_data cephfs_data --yes-i-really-really-mean-it', **get_subprocess_kwargs(silent))
        subprocess.call('sudo ceph osd pool rm cephfs_metadata cephfs_metadata --yes-i-really-really-mean-it', **get_subprocess_kwargs(silent))
        subprocess.call('sudo ceph osd pool rm device_health_metrics device_health_metrics --yes-i-really-really-mean-it', **get_subprocess_kwargs(silent))


def create_pools(placement_groups, silent):
    '''Create ceph pools.'''
    try:
        subprocess.check_call('sudo ceph osd pool create cephfs_data {0} {0}'.format(placement_groups), **get_subprocess_kwargs(silent))
        subprocess.check_call('sudo ceph osd pool create cephfs_metadata {0} {0}'.format(placement_groups), **get_subprocess_kwargs(silent))
        subprocess.check_call('sudo ceph osd pool set cephfs_data pg_autoscale_mode off', **get_subprocess_kwargs(silent))
        subprocess.check_call('sudo ceph fs new cephfs cephfs_metadata cephfs_data', **get_subprocess_kwargs(silent))
        return True
    except Exception as e:
        printe('Experienced error: {}'.format(e))
        return False
