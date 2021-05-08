import subprocess

import remoto.process


'''Utility functions to control managers.
Requires:
    Executor (executor)
    rados_util'''

def start_managers(managers, ceph_deploypath, silent):
    '''Create and start manager nodes.'''
    cmd = '{} --overwrite-conf mgr create {}'.format(ceph_deploypath, ' '.join(x.hostname for x in managers))
    return subprocess.call(cmd, **get_subprocess_kwargs(silent)) == 0


def stop_manager(manager, connection, silent):
    '''Stops a single manager.'''
    _, _, _ = remoto.process.check(connection, 'sudo systemctl stop ceph-mgr.target', shell=True)


def restart_managers(managers, silent):
    '''Restarts managers. An essential feature for when you modify configs and need to reload for changes to take effect.'''
    executors = [Executor('ssh {} "sudo systemctl restart ceph-mgr.target"'.format(x.hostname), **get_subprocess_kwargs(silent)) for x in managers]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)