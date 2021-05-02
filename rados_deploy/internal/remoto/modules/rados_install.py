import os
import subprocess

from metareserve import Reservation

from designation import Designation
import internal.util.fs as fs
import internal.util.importer as importer

def install_ceph_deploy(location):
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
    return subprocess.call('pip3 install . --user', shell=True, cwd=fs.join(location, 'ceph-deploy'), **self.call_opts()) == 0


def install_ssh_keys(reservation_str, key, user, use_sudo=True):
    '''Adds an SSH entry in the SSH config of this node, for each info.
    Note: This only has to be executed on 1 node, which will be designated the `ceph admin node`. Executing it on all nodes might be more comfortable, as it allows full n-to-n-to-n-to... jumps.
    Args:
        reservation_str (str): String representation of reservation, containing all nodes of the cluster for which we install RADOS-ceph.
        key (str): Private key to use when connecting to remote. Sending a private key is a little dodgy, until you think: 
                   This connection is used over SSH, and has equivalent protective  measures as SSL.
                   There is as much risk involved as entering banking credentials on your (SSL-secured) bank site.
                   If you know a better way to make n-to-n SSH communication possible, make a pull request.
        user (str): Username (must be the same for each node).
        use_sudo (optional bool): If set, also installs SSH keys for the root user.

    Returns:
        `True` on success, `False` on failure.'''
    reservation = Reservation.from_string(reservation_str)
    home = os.path.expanduser('~/')

    fs.mkdir('{}/.ssh'.format(home), exist_ok=True)

    if fs.isfile('{}/.ssh/config'.format(home)):
        with open('{}/.ssh/config'.format(home)) as f:
            hosts_available = [line[5:].strip().lower() for line in f.readlines() if line.startswith('Host ')]
    else:
        hosts_available = []
    neededinfo = sorted(list(x for x in reservation.nodes if x.hostname.lower() not in hosts_available), key=lambda x: x.hostname)

    with open('{}/.ssh/rados_deploy.rsa', 'w') as f:
        f.write(key)

    config = ''.join('''
Host {0}
    Hostname {0}
    User {1}
    IdentityFile {2}/.ssh/rados_deploy.rsa
    StrictHostKeyChecking accept-new
'''.format(x.hostname, user, home) for x in neededinfo)
    with open('{}/.ssh/config'.format(home), 'a') as f:
        f.write(config)
    return subprocess.call('sudo cp {}/.ssh/config /root/.ssh/'.format(home), shell=True) == 0 if use_sudo else True


def install_ceph(reservation_str, silent=False):
    '''Installs ceph on all nodes. Requires updated package manager.
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Warning: Expects to find a 'designations' extra-info key, with as value a comma-separated string for each node in the reservation, listing its designations. 
             E.g. node.extra_info['designations'] = 'mon,mds,osd,osd'
             Note: osd designation may be repeated.
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
        printe('Not every node has required "designations" extra info set.')
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
        printe('Could not scp Arrow libraries to all nodes.')
        return False

    executors = [Executor('ssh {} "sudo cp {}/libcls* /usr/lib/rados-classes/"'.format(x.hostname, home), **kwargs) for x in reservation.nodes]
    executors += [Executor('ssh {} "sudo cp {}/libarrow* /usr/lib/"'.format(x.hostname, home), **kwargs) for x in reservation.nodes]
    executors += [Executor('ssh {} "sudo cp {}/libparquet* /usr/lib/"'.format(x.hostname, home), **kwargs) for x in reservation.nodes]
    
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        printe('Could not copy libraries to destinations on all nodes.')
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