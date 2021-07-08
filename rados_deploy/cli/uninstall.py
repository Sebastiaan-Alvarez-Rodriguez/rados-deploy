import rados_deploy.cli.util as _cli_util
import rados_deploy.uninstall as _uninstall


'''CLI module to install Ceph and RADOS-Ceph on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    uninstallparser = subparsers.add_parser('uninstall', help='Teardown RADOS-Ceph environment on server cluster.')
    uninstallparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    return [uninstallparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'uninstall'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _uninstall(reservation, install_dir=args.install_dir, key_path=args.key_path, admin_id=args.admin_id, silent=args.silent) if reservation else False