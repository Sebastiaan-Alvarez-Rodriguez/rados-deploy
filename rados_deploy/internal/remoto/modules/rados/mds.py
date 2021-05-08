import subprocess

import remoto.process


'''Utility functions to control metadata servers.
Requires:
    Executor (executor)
    rados_util'''


def start_mdss(mdss, ceph_deploypath, silent):
    '''Starts metadata servers.'''
    cmd = '{} mds create {}'.format(ceph_deploypath, ' '.join(x.hostname for x in mdss))
    return subprocess.call(cmd, **get_subprocess_kwargs(silent)) == 0


def stop_mds(mds, connection, silent):
    '''Stops a (!)single(!) metadata server. Does not return anything.'''
    _, _, _ = remoto.process.check(connection, 'sudo systemctl stop ceph-mds.target', shell=True)


def restart_mdss(mdss, silent):
    '''Restarts managers. An essential feature for when you modify configs and need to reload for changes to take effect.'''
    executors = [Executor('ssh {} "sudo systemctl restart ceph-mds.target"'.format(x.hostname), **get_subprocess_kwargs(silent)) for x in mdss]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)