import cli.util as _cli_util
import install as _install


'''CLI module to install Ceph and RADOS-Ceph on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    uninstallparser = subparsers.add_parser('uninstall', help='Orchestrate RADOS-Ceph environment on server cluster.')
    uninstallparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the node that will be the Ceph admin node.')
    uninstallparser.add_argument('--cores', metavar='amount', type=int, default=_install._default_cores(), help='Amount of cores to use for compiling on remote nodes (default={}).'.format(_install._default_cores()))
    uninstallparser.add_argument('--use-sudo', metavar='bool', dest='use_sudo', help='If set, uses superuser-priviledged commands during installation. Otherwise, performs local installs, no superuser privileges required.')
    uninstallparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    uninstallparser.add_argument('--retries', metavar='amount', type=int, default=_install._default_retries(), help='Amount of retries to use for risky operations (default={}).'.format(_install._default_retries()))
    return [uninstallparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'uninstall'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _install.install(reservation, args.installdir, args.key_path, args.admin_id, cluster_keypair=None, silent=args.silent, use_sudo=args.use_sudo, cores=args.cores) if reservation else False