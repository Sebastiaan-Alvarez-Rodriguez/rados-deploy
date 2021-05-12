import rados_deploy.internal.defaults.data as defaults
import rados_deploy.internal.defaults.start as start_defaults
import rados_deploy.cli.util as _cli_util
from rados_deploy import deploy as _deploy


'''CLI module to deploy data on a RADOS-Ceph cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    deployparser = subparsers.add_parser('deploy', help='Deploy data on a RADOS-Ceph cluster.')
    deployparser.add_argument('paths', metavar='path', type=str, nargs='+', help='Data path(s) to deploy on the remote cluster.')
    deployparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the Ceph admin node.')
    deployparser.add_argument('--mountpoint', metavar='path', type=str, default=start_defaults.mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(start_defaults.mountpoint_path()))
    deployparser.add_argument('--stripe', metavar='amount', type=int, default=defaults.stripe(), help='Striping, in megabytes (default={}MB). Must be a multiple of 4. Make sure that every file is smaller than set stripe size.'.format(defaults.stripe()))
    deployparser.add_argument('--multiplier', metavar='amount', type=int, default=1, help='Data multiplier (default=1). Every file copied will receive "amount"-1 of hardlinks, to make the data look "amount" times larger.')
    deployparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    return [deployparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.subcommand == 'deploy'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _deploy(reservation, paths=args.paths, key_path=args.key_path, admin_id=args.admin_id, stripe=args.stripe, multiplier=args.multiplier, mountpoint_path=args.mountpoint, silent=args.silent) if reservation else False