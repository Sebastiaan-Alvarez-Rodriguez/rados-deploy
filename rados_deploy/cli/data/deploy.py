import cli.util as _cli_util
import data as _data

'''CLI module to deploy data on a RADOS-Ceph cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    deployparser = subparsers.add_parser('deploy', help='Deploy data on a RADOS-Ceph a cluster.')
    deployparser.add_argument('paths', metavar='paths', type=str, nargs='+', help='Data path(s) to deploy on the remote cluster.')
    deployparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the Ceph admin node.')
    deployparser.add_argument('--mountpoint', metavar='path', type=str, default=_data._default_mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(_data._default_mountpoint_path()))
    deployparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    deployparser.add_argument('--stripe', metavar='amount', type=int, default=_data._default_stripe(), help='Striping, in bytes (default={}).'.format(_data._default_stripe()))
    return [deployparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.subcommand == 'deploy'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _data.deploy(reservation, args.key_path, args.paths, args.stripe, args.admin_id, mountpoint_path=args.mountpoint, silent=args.silent) if reservation else False