import subprocess

import remoto.process


'''Utility functions to control monitors.
Requires:
    Executor (executor)
    rados_util'''

def create_monitors(monitors, ceph_deploypath, silent):
    '''Creates new monitor nodes.'''
    cmd = '{} new {}'.format(ceph_deploypath, ' '.join(x.name for x in monitors))
    return subprocess.call(cmd, **get_subprocess_kwargs(silent)) == 0


def start_monitors(ceph_deploypath, silent):
    '''Start monitor nodes.'''
    cmd = '{} --overwrite-conf mon create-initial'.format(ceph_deploypath)
    return subprocess.call(cmd, **get_subprocess_kwargs(silent)) == 0


def stop_monitor(monitor, connection, silent):
    '''Stops monitors. Does not return anything.'''
    out, err, code = remoto.process.check(connection, 'sudo systemctl stop ceph-mon.target', shell=True)
    return code == 0
    # executors = [Executor('ssh {} "sudo systemctl stop ceph-mon.target"'.format(x.name), **get_subprocess_kwargs(silent)) for x in monitors]
    # Executor.run_all(executors)
    # Executor.wait_all(executors, stop_on_error=False, print_on_error=False)


def restart_monitors(monitors, silent):
    '''Restarts monitors. An essential feature for when you modify configs and need to reload for changes to take effect.'''
    executors = [Executor('ssh {} "sudo systemctl restart ceph-mon.target"'.format(x.name), **get_subprocess_kwargs(silent)) for x in monitors]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)
