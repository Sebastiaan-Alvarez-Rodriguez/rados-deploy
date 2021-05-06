import internal.util.fs as fs


def cephdeploydir(installdir):
    '''Path to ceph-deploy source directory.'''
    return fs.join(installdir, 'ceph-deploy')


def arrowdir(installdir):
    '''Path to RADOS-arrow compilation directory.'''
    return fs.join(installdir, 'arrow')