import rados_deploy.internal.defaults.start as defaults
import rados_deploy.cli.util as _cli_util

from rados_deploy.internal.util.printer import *

'''CLI module to start RADOS-Ceph on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    startparser = subparsers.add_parser('start', help='Start RADOS-Ceph on a cluster.')
    startparser.add_argument('--mountpoint', metavar='path', type=str, default=defaults.mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(defaults.mountpoint_path()))
    startparser.add_argument('--osd-op-threads', metavar='amount', dest='osd_op_threads', type=int, default=defaults.osd_op_threads(), help='Number of op threads to use for each OSD (default={}). Make sure this number is not greater than the amount of cores each OSD has.'.format(defaults.osd_op_threads()))
    startparser.add_argument('--osd-pool-size', metavar='amount', dest='osd_pool_size', type=int, default=defaults.osd_pool_size(), help='Fragmentation of objects across this number of OSDs (default={}).'.format(defaults.osd_pool_size()))
    startparser.add_argument('--osd-max-obj-size', metavar='bytes', dest='osd_max_obj_size', type=int, default=defaults.osd_max_obj_size(), help='Maximum size (in bytes) for a single object (default={}). If we try to write objects larger than this size, the cluster will permanently hang.'.format(defaults.osd_max_obj_size()))
    startparser.add_argument('--placement-groups', metavar='amount', dest='placement_groups', type=int, default=None, help='Amount of placement groups in Ceph. By default, we use the formula `(num osds * 100) / (pool size)`, as found here: https://ceph.io/pgcalc/.'.format(defaults.mountpoint_path()))
    startparser.add_argument('--disable-client-cache', dest='disable_client_cache', help='If set, disables the I/O cache on the clients.')
    startparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    startparser.add_argument('--retries', metavar='amount', type=int, default=defaults.retries(), help='Amount of retries to use for risky operations (default={}).'.format(defaults.retries()))

    subsubparsers = startparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    
    memstoreparser = subsubparsers.add_parser('memstore', help='''Start a memstore cluster.
Memstore stores all data inside the RAM of each Ceph OSD node.''')
    memstoreparser.add_argument('--storage-size', metavar='amount', dest='storage_size', type=str, default=None, help='Amount of bytes of RAM to allocate for storage with memstore (default={}). Value should not be greater than the amount of RAM available on each OSD node.'.format(defaults.memstore_storage_size()))
    
    bluestoreparser = subsubparsers.add_parser('bluestore', help='''Start a bluestore cluster.
Bluestore stores all data on a separate device, using its own filesystem.
Each node must provide extra info:
 - device_path: Path to storage device, e.g. "/dev/nvme0n1p4".''')
    bluestoreparser.add_argument('--device-path', metavar='path', dest='device_path', type=str, default=None, help='Overrides "device_path" specification for all nodes.')
    bluestoreparser.add_argument('--use-ceph-volume', metavar='bool', dest='use_ceph_volume', type=bool, default=False, help='Use ceph-volume command for osds')
    
    return [startparser, memstoreparser, bluestoreparser]

def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'start'

def deploy(parsers, args):
    if args.subcommand == 'memstore':
        from rados_deploy.start import memstore
        reservation = _cli_util.read_reservation_cli()
        return memstore(reservation, key_path=args.key_path, admin_id=args.admin_id, mountpoint_path=args.mountpoint, osd_op_threads=args.osd_op_threads, osd_pool_size=args.osd_pool_size, osd_max_obj_size=args.osd_max_obj_size, placement_groups=args.placement_groups, use_client_cache=not args.disable_client_cache, storage_size=args.storage_size, silent=args.silent, retries=args.retries)[0] if reservation else False
    elif args.subcommand == 'bluestore':
        from rados_deploy.start import bluestore
        reservation = _cli_util.read_reservation_cli()
        return bluestore(reservation, key_path=args.key_path, admin_id=args.admin_id, mountpoint_path=args.mountpoint, osd_op_threads=args.osd_op_threads, osd_pool_size=args.osd_pool_size, osd_max_obj_size=args.osd_max_obj_size, placement_groups=args.placement_groups, use_client_cache=not args.disable_client_cache, device_path=args.device_path, use_ceph_volume=args.use_ceph_volume, silent=args.silent, retries=args.retries)[0] if reservation else False
    else: # User did not specify what type of storage type to use.
        printe('Did not provide a storage type (e.g. bluestore).')
        parsers[0].print_help()
        return False