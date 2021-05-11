import rados_deploy.internal.defaults.start as start_defaults
import rados_deploy.cli.util as _cli_util
import rados_deploy.stop as _stop


'''CLI module to stop a running RADOS-Ceph cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    stopparser = subparsers.add_parser('stop', help='Stop RADOS-Ceph on a cluster.')
    stopparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the Ceph admin node.')
    stopparser.add_argument('--mountpoint', metavar='path', type=str, default=start_defaults.mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(start_defaults.mountpoint_path()))
    stopparser.add_argument('--silent', help='If set, less output is shown.', action='store_true')
    return [stopparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'stop'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _stop(reservation, args.key_path, args.admin_id, mountpoint_path=args.mountpoint, silent=args.silent) if reservation else False