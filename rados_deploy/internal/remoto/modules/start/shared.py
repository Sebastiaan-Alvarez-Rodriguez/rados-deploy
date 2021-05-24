
def determine_config_type(config_parser):
    '''Determines the type of a ceph.conf file and returns it.
    Args:
        config_parser (configparser): Parser object with read in ceph.conf to check.

    Returns:
        rados_deploy.StorageType of the ceph.conf.'''
    if 'memstore device bytes' in config_parser['global']:
        return StorageType.MEMSTORE
    else:
        return StorageType.BLUESTORE