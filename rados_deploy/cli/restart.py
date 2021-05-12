import rados_deploy.internal.defaults.start as start_defaults
import rados_deploy.internal.defaults.restart as defaults
import rados_deploy.cli.util as _cli_util
import rados_deploy.restart as _restart


'''CLI module to start RADOS-Ceph on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    restartparser = subparsers.add_parser('restart', help='Restart a RADOS-Ceph cluster.')
    restartparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the Ceph admin node.')
    restartparser.add_argument('--mountpoint', metavar='path', type=str, default=start_defaults.mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(start_defaults.mountpoint_path()))
    restartparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    restartparser.add_argument('--retries', metavar='amount', type=int, default=defaults.retries(), help='Amount of retries to use for risky operations (default={}).'.format(defaults.retries()))
    return [restartparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'restart'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _restart(reservation, args.key_path, args.admin_id, mountpoint_path=args.mountpoint, silent=args.silent, retries=args.retries)[0] if reservation else False