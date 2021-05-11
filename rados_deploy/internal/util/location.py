import rados_deploy.internal.util.fs as fs


def cephdeploydir(install_dir):
    '''Path to ceph-deploy source directory.'''
    return fs.join(install_dir, 'ceph-deploy')


def arrowdir(install_dir):
    '''Path to RADOS-arrow compilation directory.'''
    return fs.join(install_dir, 'arrow')