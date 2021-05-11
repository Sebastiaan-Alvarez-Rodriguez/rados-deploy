import rados_deploy.internal.defaults.start as start_defaults
import rados_deploy.cli.util as _cli_util
from rados_deploy import clean as _clean
'''CLI module to clean data from a RADOS-Ceph cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    deployparser = subparsers.add_parser('clean', help='Clean data from a RADOS-Ceph a cluster.')
    deployparser.add_argument('paths', metavar='paths', type=str, nargs='*', help='Data path(s) to clean on the remote cluster (mountpoint path will be prepended). If no paths given, removes all data on remote.')
    deployparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the Ceph admin node.')
    deployparser.add_argument('--mountpoint', metavar='path', type=str, default=start_defaults.mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(start_defaults.mountpoint_path()))
    deployparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    return [deployparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.subcommand == 'clean'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _clean(reservation, args.key_path, args.paths, args.admin_id, mountpoint_path=args.mountpoint, silent=args.silent) if reservation else False