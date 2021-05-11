import cli.util as _cli_util
import data as _data

'''CLI module to deploy data generators on a RADOS-Ceph cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    deployparser = subparsers.add_parser('deploy', help='Deploy data generators on a RADOS-Ceph cluster.')
    submitparser.add_argument('cmd', metavar='cmd', type=str, help='Command to execute on the remote cluster. Note: $JAVA_HOME/bin/java is available for java applications. python3 is available for python applications. If you need to use flags in the command with "-" signs, use e.g. "-- -h" to ignore "-" signs for the rest of the command.')
    deployparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the Ceph admin node.')
    deployparser.add_argument('--paths', metavar='path', type=str, nargs='+', help='Path(s) to applications to deploy on the remote cluster. Given applications will be available in the CWD for command execution.')
    deployparser.add_argument('--mountpoint', metavar='path', type=str, default=_data._default_mountpoint_path(), help='Mountpoint for CephFS on all nodes (default={}).'.format(_data._default_mountpoint_path()))
    deployparser.add_argument('--stripe', metavar='amount', type=int, default=_data._default_stripe(), help='Striping, in megabytes (default={}MB). Must be a multiple of 4. Make sure that every file is smaller than set stripe size.'.format(_data._default_stripe()))
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
    return _data.generate(reservation, args.key_path, args.admin_id, args.cmd, args.paths, args.stripe, args.multiplier, mountpoint_path=args.mountpoint, silent=args.silent) if reservation else False