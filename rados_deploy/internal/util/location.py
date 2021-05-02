import internal.util.fs as fs


def sparkdir(installdir):
    '''Path to Spark installation.'''
    return fs.join(installdir, 'spark')


def java_nonroot_dir(installdir):
    '''Path to non-root java installation. Warning: If this system detected java is already installed, it will not install java again, and this dir will not exist.'''
    return fs.join(installdir, 'java')