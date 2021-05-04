import cli.util as _cli_util
import install as _install


'''CLI module to install Ceph and RADOS-Ceph on a cluster.'''

def subparser(subparsers):
    '''Register subparser modules'''
    installparser = subparsers.add_parser('install', help='Orchestrate Spark environment on server cluster.')
    installparser.add_argument('--spark-url', dest='spark_url', type=str, default=_install._default_spark_url(), help='Spark download URL.')
    installparser.add_argument('--java-url', dest='java_url', type=str, default=_install._default_java_url(), help='Java download URL. Make sure the downloaded version is acceptable (between [`java-min`, `java-max`])')
    installparser.add_argument('--java-min', dest='java_min', type=int, default=_install._default_java_min(), help='Java minimal version (default={}). 0 means "no limit". use this to ensure a recent-enough version is installed for use with your Spark version.'.format(_install._default_java_min()))
    installparser.add_argument('--java-max', dest='java_max', type=int, default=_install._default_java_max(), help='Java minimal version (default={}). 0 means "no limit". use this to ensure a recent-enough version is installed for use with your Spark version.'.format(_install._default_java_max()))
    installparser.add_argument('--use-sudo', dest='use_sudo', help='If set, uses superuser-priviledged commands during installation. Otherwise, performs local installs, no superuser privileges required.')
    installparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    installparser.add_argument('--retries', metavar='amount', type=int, default=_install._default_retries(), help='Amount of retries to use for risky operations (default={}).'.format(_install._default_retries()))
    return [installparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'install'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _install.install(reservation, args.installdir, args.key_path, args.spark_url, args.java_url, args.java_min, args.java_max) if reservation else False