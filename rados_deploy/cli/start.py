import cli.util as _cli_util
import start as _start


'''CLI module to start RADOS-Ceph on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    startparser = subparsers.add_parser('start', help='Start RADOS-Ceph on a cluster.')
    startparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the node that will be the Ceph admin node.')
    startparser.add_argument('--mountpoint', metavar='path', type=str, default=_start._default_mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(_start._default_mountpoint_path()))
    startparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    startparser.add_argument('--retries', metavar='amount', type=int, default=_start._default_retries(), help='Amount of retries to use for risky operations (default={}).'.format(_start._default_retries()))
    return [startparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'start'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _start.start(reservation, args.key_path, args.admin_id, mountpoint_path=args.mountpoint, silent=args.silent, retries=args.retries) if reservation else False