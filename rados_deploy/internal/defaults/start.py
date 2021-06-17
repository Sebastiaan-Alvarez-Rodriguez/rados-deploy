def mountpoint_path():
    return '/mnt/cephfs'

def retries():
    return 10

def osd_op_threads():
    return 4

def osd_pool_size():
    return 3

def osd_max_obj_size():
    return 128*1024*1024

## memstore defaults
def memstore_storage_size():
    return '10GiB'