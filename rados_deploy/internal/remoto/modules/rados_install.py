import os
import subprocess

from metareserve import Reservation

from designation import Designation
from internal.util.executor import Executor
import internal.util.fs as fs
import internal.util.importer as importer


def stderr(string, *args, **kwargs):
    kwargs['flush'] = True
    kwargs['file'] = sys.stderr
    print('[{}] {}'.format(socket.gethostname(), string), *args, **kwargs)


def install_ceph_deploy(location, silent=False):
    '''Install ceph-deploy on the admin node. Warning: Assumes `git` is installed and available.
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Args:
        location (str): Location to install ceph-deploy in. Ceph-deploy root will be`location/ceph-deploy`.

    Returns:
        `True` on success, `False` on failure.'''
    if importer.library_exists('ceph_deploy'):
        return True
    if not importer.pip_install():
        return False

    if not fs.exists(location):
        if subprocess.call('git clone https://github.com/ceph/ceph-deploy', shell=True, cwd=location, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) != 0:
            return False
    kwargs = {'shell': True}
    if silent:
        kwargs['stderr'] = subprocess.DEVNULL
        kwargs['stdout'] = subprocess.DEVNULL
    return subprocess.call('pip3 install . --user', cwd=fs.join(location, 'ceph-deploy'), **kwargs) == 0


def install_ceph(reservation_str, silent=False):
    '''Installs ceph on all nodes. Requires updated package manager.
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Warning: Expects to find a 'designations' extra-info key, with as value a comma-separated string for each node in the reservation, listing its designations. 
             Daemons for the given designations will be installed.
             E.g. node.extra_info['designations'] = 'mon,mds,osd,osd' will install the monitor, metadata-server and osd daemons.
             Note: Designations may be repeated, without effect.
             Warning: Each node must have at least 1 designation.
    Warning: We assume apt package manager.
    Args:
        reservation_str (str): String representation of reservation, containing all nodes of the cluster for which we install RADOS-ceph.

    Returns:
        `True` on success, `False` on failure.'''
    reservation = Reservation.from_string(reservation_str)
    home = os.path.expanduser('~/')
    ceph_deploypath = '{}/.local/bin/ceph-deploy'.format(home)

    if any(x for x in reservation.nodes if not 'designations' in x.extra_info):
        stderr('Not every node has required "designations" extra info set.')
        return False

    kwargs = {'shell': True}
        if silent:
            kwargs['stderr'] = subprocess.DEVNULL
            kwargs['stdout'] = subprocess.DEVNULL

    if subprocess.call('sudo apt update -y', **kwargs) != 0:
        return False


    executors = [Executor('{} --overwrite-conf install --release octopus {} {}'.format(ceph_deploypath, '--'+' --'.join([y.name.lower() for y in set(Designation[d] for d in x.extra_info['designations'].split(','))]), x.hostname), **kwargs) for x in reservation.nodes]
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)


def install_rados(location, reservation_str, cores=16, silent=False):
    '''Installs RADOS-arrow, which we need for bridging with Arrow. This function should be executed from the admin node. 
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Warning: Assumes apt package manager.
    Args:
        location (str): Location to install RADOS-arrow in. Ceph-deploy root will be`location/ceph-deploy`.
        reservation_str (str): String representation of reservation, containing all nodes of the cluster for which we install RADOS-ceph.
        cores (optional int): Number of cores to use for compiling (default=4). 
                              Note: Do not set this to a higher value than the number of available cores, as it would only lead to slowdowns.
                                    If set too high, it may happen that RAM consumption is much too high, leading to kernel panic and termination of critical processes.
        silent (optional bool): If set, does not print compilation progress, output, etc. Otherwise, all output will be available.
    Returns:
        `True` on success, `False` on failure.'''
    reservation = Reservation.from_string(reservation_str)
    home = os.path.expanduser('~/')
    dest = fs.join(location, 'arrow')

    kwargs = {'shell': True}
    if silent:
        kwargs['stderr'] = subprocess.DEVNULL
        kwargs['stdout'] = subprocess.DEVNULL

    if not fs.isdir('{}/cpp/build/latest'.format(dest)):
        if subprocess.call('sudo apt install libradospp-dev rados-objclass-dev openjdk-8-jdk openjdk-11-jdk libboost-all-dev automake bison flex g++ git libevent-dev libssl-dev libtool make pkg-config maven cmake thrift-compiler -y', **kwargs) != 0:
            return False
        if (not fs.isdir(dest)) and subprocess.call('git clone https://github.com/Sebastiaan-Alvarez-Rodriguez/arrow.git -b merge_bridge_dev', cwd=location, **kwargs) != 0:
            return False
        if subprocess.call('cmake . -DARROW_PARQUET=ON -DARROW_DATASET=ON -DARROW_JNI=ON -DARROW_ORC=ON -DARROW_CSV=ON -DARROW_CLS=ON', cwd='{}/cpp'.format(dest), **kwargs) != 0:
            return False
        if subprocess.call('sudo make install -j{}'.format(cores), cwd='{}/cpp'.format(dest), **kwargs) != 0:
            return False

    executors = [Executor('scp {}/cpp/build/latest/libcls* {}:~/'.format(dest, x.hostname), **kwargs) for x in reservation.nodes]
    executors += [Executor('scp {}/cpp/build/latest/libarrow* {}:~/'.format(dest, x.hostname), **kwargs) for x in reservation.nodes]
    executors += [Executor('scp {}/cpp/build/latest/libparquet* {}:~/'.format(dest, x.hostname), **kwargs) for x in reservation.nodes]
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        stderr('Could not scp Arrow libraries to all nodes.')
        return False

    executors = [Executor('ssh {} "sudo cp {}/libcls* /usr/lib/rados-classes/"'.format(x.hostname, home), **kwargs) for x in reservation.nodes]
    executors += [Executor('ssh {} "sudo cp {}/libarrow* /usr/lib/"'.format(x.hostname, home), **kwargs) for x in reservation.nodes]
    executors += [Executor('ssh {} "sudo cp {}/libparquet* /usr/lib/"'.format(x.hostname, home), **kwargs) for x in reservation.nodes]
    
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        stderr('Could not copy libraries to destinations on all nodes.')
        return False

    libpath = os.getenv('LD_LIBRARY_PATH')
    if libpath == None or not '/usr/local/lib' in libpath.strip().split(':'):
        with open('{}/.bashrc'.format(home), 'a') as f:
            if libpath == None:
                f.write('export LD_LIBRARY_PATH=/usr/local/lib\n')
            else:
                f.write('export LD_LIBRARY_PATH=/usr/local/lib:$LD_LIBRARY_PATH\n')
        os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib' if not libpath else '/usr/local/lib:'+libpath
    return subprocess.call('sudo cp /usr/local/lib/libparq* /usr/lib/', **kwargs) == 0