import cli.util as _cli_util

'''CLI module to interact with data on a RADOS-Ceph cluster.'''


def _get_modules():
    import cli.data.deploy as deploy
    return [deploy]


def subparser(subparsers):
    '''Register subparser modules'''
    dataparser = subparsers.add_parser('data', help='Data commands here...')
    subsubparsers = dataparser.add_subparsers(help='Subsubcommands', dest='subcommand')
    return [dataparser]+[x.subparser(subsubparsers) for x in _get_modules()]


def deploy_args_set(args):
    '''Indicates whether we will handle command parse output in this module.
    `deploy()` function will be called if set.

    Returns:
        `True` if we found arguments used by this subsubparser, `False` otherwise.'''
    return args.command == 'data'


def deploy(parsers, args):
    '''Processing of deploy commandline args occurs here.'''
    for parsers_for_module, module in zip(parsers[1:], _get_modules()):
        if module.deploy_args_set(args):
            return module.deploy(parsers_for_module, args)
    persers[0].print_help()
    return False