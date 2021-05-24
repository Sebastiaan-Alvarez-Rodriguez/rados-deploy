def mountpoint_path():
    return '/mnt/cephfs'

def retries():
    return 10

def osd_op_threads():
    return 4

def osd_pool_size():
    return 3


## memstore defaults
def memstore_storage_size():
    return '10GiB'