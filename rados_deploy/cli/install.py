import rados_deploy.internal.defaults.install as defaults
import rados_deploy.cli.util as _cli_util
from rados_deploy import install_ssh as _install_ssh
import rados_deploy.install as _install


'''CLI module to install Ceph and RADOS-Ceph on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    installparser = subparsers.add_parser('install', help='Orchestrate RADOS-Ceph environment on server cluster.')
    installparser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the node that will be the Ceph admin node.')
    installparser.add_argument('--arrow-url', metavar='url', dest='arrow_url', type=str, default=defaults.arrow_url(), help='Arrow download URL. Defaults to Arrow with JNI bridge and RADOS-Ceph connector.')
    
    installparser.add_argument('--cores', metavar='amount', type=int, default=defaults.cores(), help='Amount of cores to use for compiling on remote nodes (default={}).'.format(defaults.cores()))
    installparser.add_argument('--use-sudo', metavar='bool', dest='use_sudo', help='If set, uses superuser-priviledged commands during installation. Otherwise, performs local installs, no superuser privileges required.')
    installparser.add_argument('--force-reinstall', dest='force_reinstall', help='If set, we always will re-download and install Arrow. Otherwise, we will skip installing if we already have installed Arrow.', action='store_true')
    installparser.add_argument('--debug', dest='debug', help='If set, we compile Arrow using debug flags.', action='store_true')
    installparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    installparser.add_argument('--retries', metavar='amount', type=int, default=defaults.retries(), help='Amount of retries to use for risky operations (default={}).'.format(defaults.retries()))
    return [installparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'install'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    if not reservation:
        return False
    if not _install_ssh(reservation, key_path=args.key_path, cluster_keypair=None, silent=args.silent, use_sudo=args.use_sudo):
        return False
    return _install(reservation, install_dir=args.install_dir, key_path=args.key_path, admin_id=args.admin_id, arrow_url=args.arrow_url, use_sudo=args.use_sudo, force_reinstall=args.force_reinstall, debug=args.debug, silent=args.silent, cores=args.cores)[0] if reservation else False