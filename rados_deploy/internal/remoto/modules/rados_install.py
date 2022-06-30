import os
import subprocess
import tempfile
import urllib.request


def _get_ceph_deploy(location, silent=False, retries=5):
    url = 'https://github.com/ceph/ceph-deploy/archive/refs/heads/master.zip'
    with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded archive.
        archiveloc = join(tmpdir, 'ceph-deploy.zip')
        if not silent:
            print('Fetching ceph-deploy from {}'.format(url))
        for x in range(retries):
            try:
                try:
                    rm(archiveloc)
                except Exception as e:
                    pass
                urllib.request.urlretrieve(url, archiveloc)
                break
            except Exception as e:
                if x == 0:
                    printw('Could not download ceph-deploy. Retrying...')
                elif x == retries-1:
                    printe('Could not download ceph-deploy: {}'.format(e))
                    return False
        try:
            extractloc = join(tmpdir, 'extracted')
            mkdir(extractloc, exist_ok=True)
            unpack(archiveloc, extractloc)

            extracted_dir = next(ls(extractloc, only_dirs=True, full_paths=True)) # find out what the extracted directory is called. There will be only 1 extracted directory.
            rm(location, ignore_errors=True)
            mkdir(location)
            for x in ls(extracted_dir, full_paths=True): # Move every file and directory to the final location.
                mv(x, location)
            return True
        except Exception as e:
            printe('Could not extract ceph-deploy zip file correctly: ', e)
            return False


def _get_rados_dev(location, arrow_url, silent=False, retries=5):
    with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded archive.
        archiveloc = join(tmpdir, 'rados-arrow.zip')
        if not silent:
            print('Fetching RADOS-arrow from {}'.format(arrow_url))
        for x in range(retries):
            try:
                try:
                    rm(archiveloc)
                except Exception as e:
                    pass
                urllib.request.urlretrieve(arrow_url, archiveloc)
                break
            except Exception as e:
                if x == 0:
                    printw('Could not download RADOS-arrow. Retrying...')
                elif x == retries-1:
                    printe('Could not download RADOS-arrow: {}'.format(e))
                    return False
        try:
            extractloc = join(tmpdir, 'extracted')
            mkdir(extractloc, exist_ok=True)
            unpack(archiveloc, extractloc)

            extracted_dir = next(ls(extractloc, only_dirs=True, full_paths=True)) # find out what the extracted directory is called. There will be only 1 extracted directory.
            rm(location, ignore_errors=True)
            mkdir(location)
            for x in ls(extracted_dir, full_paths=True): # Move every file and directory to the final location.
                mv(x, location)
            return True
        except Exception as e:
            printe('Could not extract RADOS-arrow zip file correctly: {}'.format(e))
            return False


def install_ceph_deploy(location, silent=False):
    '''Install ceph-deploy on the admin node. Warning: Assumes `git` is installed and available.
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Args:
        location (str): Location to install ceph-deploy in. Ceph-deploy root will be`location/ceph-deploy`.

    Returns:
        `True` on success, `False` on failure.'''
    if library_exists('ceph_deploy'):
        return True
    if not pip_install(py='python3'):
        return False

    if not exists(location):
        if not _get_ceph_deploy(location, silent=silent):
            return False
    kwargs = {'shell': True}
    if silent:
        kwargs['stderr'] = subprocess.DEVNULL
        kwargs['stdout'] = subprocess.DEVNULL
    return subprocess.call('pip3 install . --user', cwd=location, **kwargs) == 0


def install_ceph(hosts_designations_mapping, silent=False):
    '''Installs required ceph daemons on all nodes. Requires updated package manager.
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Warning: Expects to find a 'designations' extra-info key, with as value a comma-separated string for each node in the reservation, listing its designations. 
             Daemons for the given designations will be installed. E.g. node.extra_info['designations'] = 'mon,mds,osd,osd' will install the monitor, metadata-server and osd daemons.
             Note: Designations may be repeated, which will not change behaviour from listing designations once.
    Warning: We assume apt package manager.
    Note: If a host has an empty list as specification, we ignore it and do not install anything.
    Args:
        hosts_designations_mapping (dict(str, list(str))): Dict with key=hostname and value=list of hostname's `Designations` as strings.
        hosts_user_mapping (dict(str, str)): Dict with key=hostname and val=username for host.
        silent (optional bool): If set, does not print compilation progress, output, etc. Otherwise, all output will be available.
    
    Returns:
        `True` on success, `False` on failure.'''
    ceph_deploypath = join(os.path.expanduser('~/'), '.local', 'bin', 'ceph-deploy')

    kwargs = {'shell': True, 'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL}

    if subprocess.call('sudo apt update -y', **kwargs) != 0:
        return False
    if subprocess.call('{} install --common localhost'.format(ceph_deploypath), **kwargs) != 0:
        return False

    executors = []
    for hostname, designations in hosts_designations_mapping.items():
        if not any(designations): # If no designation given for node X, we skip installation of Ceph for X.
            continue
        designation_out = '--'+' --'.join([x.lower() for x in set(designations)])
        executors.append(Executor('{} --overwrite-conf install --release octopus {} {}'.format(ceph_deploypath, designation_out, hostname), shell=True))           
    Executor.run_all(executors)
    return Executor.wait_all(executors, print_on_error=True)


def install_rados(location, hosts_designations_mapping, arrow_url, force_reinstall=False, debug=False, silent=False, cores=16):
    '''Installs RADOS-arrow, which we need for bridging with Arrow. This function should be executed from the admin node. 
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Warning: Assumes apt package manager.
    Args:
        location (str): Location to install RADOS-arrow in. Ceph-deploy root will be`location/ceph-deploy`.
        hosts_designations_mapping (dict(str, list(str))): Dict with key=hostname and value=list of hostname's `Designations` as strings.
        arrow_url (str): Download URL for Arrow library to use with RADOS-Ceph.
        force_reinstall (optional bool): If set, we always will re-download and install Arrow. Otherwise, we will skip installing if we already have installed Arrow.
        debug (optional bool): If set, we compile Arrow using debug flags.
        silent (optional bool): If set, does not print compilation progress, output, etc. Otherwise, all output will be available.
        cores (optional int): Number of cores to use for compiling (default=4). 
                              Note: Do not set this to a higher value than the number of available cores, as it would only lead to slowdowns.
                                    If set too high, it may happen that RAM consumption is much too high, leading to kernel panic and termination of critical processes.
    Returns:
        `True` on success, `False` on failure.'''
    kwargs = {'shell': True}
    if silent:
        kwargs['stderr'] = subprocess.DEVNULL
        kwargs['stdout'] = subprocess.DEVNULL

    if force_reinstall or not (exists('{}/cpp/build/latest'.format(location)) and any(ls('{}/cpp/build/latest'.format(location)))):
        if subprocess.call('sudo rm -rf {}'.format(location), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) != 0:
            printe('Could not remove all files at {}'.format(location))
            return False
        if not silent:
            print('Installing required libraries for RADOS-Ceph.\nPatience...')
        # cmd = 'sudo apt install libradospp-dev rados-objclass-dev openjdk-8-jdk openjdk-11-jdk default-jdk libboost-all-dev automake bison flex g++ libevent-dev libssl-dev \
        #     libtool make pkg-config maven cmake thrift-compiler llvm -y'
        cmd = 'sudo apt install -y python3 python3-pip python3-venv python3-numpy cmake libradospp-dev rados-objclass-dev llvm default-jdk maven'
        if subprocess.call(cmd, shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) != 0:
            printe('Failed to install all required libraries. Command used: {}'.format(cmd))
            return False
        if not silent:
            prints('Installed required libraries.')
        if (not isdir(location)) and not _get_rados_dev(location, arrow_url, silent=silent, retries=5):
            return False
        
        cmake_cmd = 'cmake -DARROW_SKYHOOK=ON -DARROW_PARQUET=ON -DARROW_WITH_SNAPPY=ON -DARROW_WITH_ZLIB=ON -DARROW_BUILD_EXAMPLES=ON -DPARQUET_BUILD_EXAMPLES=ON \
            -DARROW_PYTHON=ON -DARROW_ORC=ON -DARROW_JAVA=ON -DARROW_JNI=ON -DARROW_DATASET=ON -DARROW_CSV=ON -DARROW_WITH_LZ4=ON -DARROW_WITH_ZSTD=ON'
        if debug:
            cmake_cmd += ' -DCMAKE_BUILD_TYPE=Debug'
        print ("!!!! " + cmake_cmd + " !!!!!") 
        
        my_env = os.environ.copy()
        my_env["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
        print(my_env)
        subprocess.call(cmake_cmd+' 1>&2', cwd='{}/cpp'.format(location), env=my_env, **kwargs)
        if subprocess.call(cmake_cmd+' 1>&2', cwd='{}/cpp'.format(location), env=my_env, **kwargs) != 0:
            return False
        if subprocess.call('sudo make install -j{} 1>&2'.format(cores), cwd='{}/cpp'.format(location), **kwargs) != 0:
            return False

    hosts = [key for key, value in hosts_designations_mapping.items() if any(value)] # Only nodes joining the ceph cluster will receive the libraries

    executors = [Executor('ssh {} "mkdir -p ~/.arrow-libs/ && sudo mkdir -p /usr/lib/rados-classes/"'.format(x), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) for x in hosts]
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        printe('Could not create required directories on all nodes.')
        return False
    executors = [Executor('scp {}/cpp/build/latest/libcls* {}:~/.arrow-libs/'.format(location, x), **kwargs) for x in hosts]
    executors += [Executor('scp {}/cpp/build/latest/libarrow* {}:~/.arrow-libs/'.format(location, x), **kwargs) for x in hosts]
    executors += [Executor('scp {}/cpp/build/latest/libparquet* {}:~/.arrow-libs/'.format(location, x), **kwargs) for x in hosts]
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        printe('Could not scp Arrow libraries to all nodes.')
        return False

    executors = [Executor('ssh {} "sudo cp ~/.arrow-libs/libcls* /usr/lib/rados-classes/"'.format(x), **kwargs) for x in hosts]
    executors += [Executor('ssh {} "sudo cp ~/.arrow-libs/libarrow* /usr/lib/"'.format(x), **kwargs) for x in hosts]
    executors += [Executor('ssh {} "sudo cp ~/.arrow-libs/libparquet* /usr/lib/"'.format(x), **kwargs) for x in hosts]
    executors += [Executor('ssh {} "sudo systemctl restart ceph-osd.target"'.format(x), **kwargs) for x in hosts]
    
    Executor.run_all(executors)
    if not Executor.wait_all(executors, print_on_error=True):
        printe('Could not copy libraries to destinations on all nodes.')
        return False

    env = Environment()
    env.load_to_env()
    libpath = env.get('LD_LIBRARY_PATH')
    if not libpath:
        libpath = ''
    if not libpath or not '/usr/local/lib' in libpath.strip().split(':'):
        env.set('LD_LIBRARY_PATH', '/usr/local/lib:'+libpath)
    os.environ['LD_LIBRARY_PATH'] = '/usr/local/lib:'+libpath
    return subprocess.call('sudo cp /usr/local/lib/libparq* /usr/lib/', **kwargs) == 0
