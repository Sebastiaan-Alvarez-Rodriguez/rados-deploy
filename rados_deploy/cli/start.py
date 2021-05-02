import cli.util as _cli_util
import start as _start
from internal.util.printer import *

'''CLI module to deploy Spark on a cluster.'''




def subparser(subparsers):
    '''Register subparser modules'''
    startparser = subparsers.add_parser('start', help='Orchestrate Spark environment on server cluster.')
    startparser.add_argument('--master', metavar='node', type=int, default=None, help='ID of the node that will be the master node.')
    startparser.add_argument('--workdir', metavar='path', type=str, default=_start._default_workdir(), help='Path to Spark workdir location for all slave daemons (default={}).'.format(_start._default_workdir()))
    startparser.add_argument('--silent', help='If set, less boot output is shown.', action='store_true')
    startparser.add_argument('--retries', metavar='amount', type=int, default=_start._default_retries(), help='Amount of retries to use for risky operations (default={}).'.format(_start._default_retries()))

    
    #retries

    # startparser.add_argument('-d', '--debug-mode', dest='debug_mode', help='Run remote in debug mode', action='store_true')
    # startparser.add_argument('-dm', '--deploy-mode', dest='deploy_mode', type=str, metavar='mode', default=str(DeployMode.STANDARD), help='Deployment mode for cluster', choices=[str(x) for x in DeployMode])
    # startparser.add_argument('--internal', nargs=1, type=str, help=argparse.SUPPRESS)
    return [startparser]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'start'


def deploy(parsers, args):
    reservation = _cli_util.read_reservation_cli()
    return _start.start(reservation, args.installdir, args.key_path, args.master, args.workdir, silent=args.silent, retries=args.retries) if reservation else False