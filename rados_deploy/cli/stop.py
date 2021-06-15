import rados_deploy.internal.defaults.start as start_defaults
import rados_deploy.cli.util as _cli_util


'''CLI module to stop a running RADOS-Ceph cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    stopparser = subparsers.add_parser('stop', help='Stop RADOS-Ceph on a cluster.')
    stopparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the Ceph admin node.')
    stopparser.add_argument('--mountpoint', metavar='path', type=str, default=start_defaults.mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(start_defaults.mountpoint_path()))
    stopparser.add_argument('--silent', help='If set, less output is shown.', action='store_true')
    
    subsubparsers = stopparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    
    memstoreparser = subsubparsers.add_parser('memstore', help='''Stop a memstore cluster.''')
    bluestoreparser = subsubparsers.add_parser('bluestore', help='''Stop a bluestore cluster.''')
    return [stopparser, memstoreparser, bluestoreparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'stop'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    if args.subcommand == 'memstore':
        from rados_deploy.stop import memstore
        reservation = _cli_util.read_reservation_cli()
        return memstore(reservation, key_path=args.key_path, admin_id=args.admin_id, mountpoint_path=args.mountpoint, silent=args.silent)[0] if reservation else False
    elif args.subcommand == 'bluestore':
        from rados_deploy.stop import bluestore
        reservation = _cli_util.read_reservation_cli()
        return bluestore(reservation, key_path=args.key_path, admin_id=args.admin_id, mountpoint_path=args.mountpoint, silent=args.silent)[0] if reservation else False
    else: # User did not specify what type of storage type to use.
        printe('Did not provide a storage type (e.g. bluestore).')
        parsers[0].print_help()
        return False