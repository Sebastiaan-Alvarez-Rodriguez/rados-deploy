import builtins
from enum import Enum
import importlib
import os
import socket
import subprocess
import sys
import tempfile
import threading
import urllib.request


##########################################################################################
# Here, we copied the contents from designation (as we cannot use local imports)
class Designation(Enum):
    OSD = 0,
    MON = 1,
    MGR = 2,
    MDS = 4

    @staticmethod
    def toint(designations):
        ans = 0
        for x in designations:
            ans |= x
        return ans

    @staticmethod
    def fromint(integer):
        if not isinstance(integer, int):
            integer = int(integer)
        return [x for x in Designation if x.value & integer != 0]

##########################################################################################
# Here, we copied the contents from internal.util.executor (as we cannot use local imports)
class Executor(object):
    '''Object to run subprocess commands in a separate thread. This way, Python can continue operating while interacting  with subprocesses.'''
    def __init__(self, cmd, **kwargs):
        self.cmd = cmd
        self.started = False
        self.stopped = False
        self.thread = None
        self.process = None
        self.kwargs = kwargs

    def run(self):
        '''Run command. Returns immediately after booting a thread'''
        if self.started:
            raise RuntimeError('Executor already started. Make a new Executor for a new run')
        if self.stopped:
            raise RuntimeError('Executor already stopped. Make a new Executor for a new run')
        if self.kwargs == None:
            self.kwargs = kwargs

        def target(**kwargs):
            self.process = subprocess.Popen(self.cmd, **kwargs)
            self.process.communicate()
            self.stopped = True

        self.thread = threading.Thread(target=target, kwargs=self.kwargs)
        self.thread.start()
        self.started = True

    def run_direct(self):
        '''Run command on current thread, waiting until it completes.
        Note: Some commands never return, which will make this function non-returning.'''
        self.process = subprocess.Popen(self.cmd, **self.kwargs)
        self.started = True
        self.process.communicate()
        self.stopped = True
        return self.process.returncode

    def wait(self):
        '''Block until this executor is done.'''
        if not self.started:
            raise RuntimeError('Executor with command "{}" not yet started, cannot wait'.format(self.cmd))
        if self.stopped:
            return self.process.returncode
        self.thread.join()
        return self.process.returncode


    def stop(self):
        '''Force-stop executor, wait until done'''
        if self.started and not self.stopped:
            if self.thread.is_alive():
                #If command fails, or when stopping directly after starting
                for x in range(5):
                    if self.process == None:
                        time.sleep(1)
                    else:
                        break
                if self.process != None:
                    self.process.terminate()
                self.thread.join()
                self.stopped = True
        return self.process.returncode if self.process != None else 1


    def reboot(self):
        '''Stop and then start wrapped command again.'''
        self.stop()
        self.started = False
        self.stopped = False
        self.run(**self.kwargs)


    def get_pid(self):
        '''Returns pid of running process, or -1 if it cannot access current process.'''
        return -1 if (not self.started) or self.stopped or self.process == None else self.process.pid


    @staticmethod
    def run_all(executors):
        '''Function to run all given executors, with same arguments.'''
        for x in executors:
            x.run()

    @staticmethod
    def __print_errors(returncodes, executors):
        if any(x!=0 for x in returncodes):
            print('Experienced errors:')
            for idx, x in enumerate(returncodes):
                if x != 0:
                    print('\treturncode: {} - command: {}'.format(x, executors[idx].cmd))

    @staticmethod
    def wait_all(executors, stop_on_error=True, return_returncodes=False, print_on_error=False):
        '''Waits for all executors before returning control.
        Args:
            stop_on_error: If set, immediately kills all remaining executors when encountering an error. Otherwise, we continue executing the other executors.
            return_returncodes: If set, returns the process returncodes. Otherwise, returns regular `True`/`False` (see below).
            print_on_error: If set, prints the command(s) responsible for errors. Otherwise, this function is silent.

        Returns:
            `True` if all processes sucessfully executed, `False` otherwise.'''
        returncodes = []
        status = True
        for x in executors:
            returncode = x.wait()
            returncodes.append(returncode)
            if returncode != 0:
                if stop_on_error: # We had an error during execution and must stop all now
                    Executor.stop_all(executors) # Stop all other executors
                    if print_on_error:
                        Executor.__print_errors(returncodes, executors)
                    return returncodes if return_returncodes else False
                else:
                    status = False
        if print_on_error and not status:
            Executor.__print_errors(returncodes, executors)
            
        return returncodes if return_returncodes else status

    @staticmethod
    def stop_all(executors, as_generator=False):
        '''Function to stop all given execuors.
        Args:
            executors: Iterable of `Executor` to stop.
            as_generator: If set, returns exit status codes as a generator. Otherwise, does not return anything.

        Returns:
            nothing by default. If `as_generator` is  set, returns the exit status code for each executor.'''
        for x in executors:
            if as_generator:
                yield x.stop()
            else:
                x.stop()

##########################################################################################
# Here, we copied (part of) the contents from internal.util.importer (as we cannot use local imports)
def library_exists(name):
    '''Check if a given library exists. Returns True if given name is a library, False otherwise.'''
    if sys.version_info >= (3, 6):
        import importlib.util
        return importlib.util.find_spec(str(name)) is not None
    if sys.version_info >= (3, 4):
        return importlib.util.find_spec(str(name)) is not None
    else:
        raise NotImplementedError('Did not implement existence check for Python 3.3 and below')

def _pip_installed(pip):
    return subprocess.call('{} -h'.format(pip), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0

def _pip_install0(py):
    return subprocess.call('{} -m ensurepip'.format(py), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0

def _pip_install1(py):
    if subprocess.call('sudo apt update -y', shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) != 0:
        return False
    return subprocess.call('sudo apt install -y {}-pip'.format(py), shell=True) == 0 #, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL

def _pip_install2(py):
    url = 'https://bootstrap.pypa.io/get-pip.py'
    with tempfile.TemporaryDirectory() as tmpdir: # We use a tempfile to store the downloaded archive.
        archiveloc = os.path.join(tmpdir, 'get-pip.py')
        if not silent:
            print('Fetching get-pip from {}'.format(url))
        for x in range(retries):
            try:
                try:
                    os.remove(archiveloc)
                except Exception as e:
                    pass
                urllib.request.urlretrieve(url, archiveloc)
                break
            except Exception as e:
                if x == 0:
                    printw('Could not download get-pip. Retrying...')
                elif x == retries-1:
                    printe('Could not download get-pip: {}'.format(e))
                    return False
        return subprocess.call('{} {}'.format(py, archiveloc), shell=True, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) == 0


def pip_install(py='python3', pip='pip3'):
    '''Installs pip. The given `pip` argument determines the name of the pip we try to get (`pip` or `pip3`). `py` argument is used when trying to install built-in pip.'''
    return __pip_installed(pip) or __pip_install0(py) or __pip_install1(py) or __pip_install2(py)


##########################################################################################
# Here, we copied the contents from internal.util.printer (as we cannot use local imports)
def print(string, *args, **kwargs):
    kwargs['flush'] = True
    kwargs['file'] = sys.stderr  # Print everything to stderr!
    return builtins.print('[{}] {}'.format(socket.gethostname(), string), *args, **kwargs)


class Color(Enum):
    '''An enum to specify what color you want your text to be'''
    RED = '\033[1;31m'
    GRN = '\033[1;32m'
    YEL = '\033[1;33m'
    BLU = '\033[1;34m'
    PRP = '\033[1;35m'
    CAN = '\033[1;36m'
    CLR = '\033[0m'

# Print given text with given color
def printc(string, color, **kwargs):
    print(format(string, color), **kwargs)

# Print given success text
def prints(string, color=Color.GRN, **kwargs):
    print('[SUCCESS] {}'.format(format(string, color)), **kwargs)

# Print given warning text
def printw(string, color=Color.YEL, **kwargs):
    print('[WARNING] {}'.format(format(string, color)), **kwargs)


# Print given error text
def printe(string, color=Color.RED, **kwargs):
    print('[ERROR] {}'.format(format(string, color)), **kwargs)


# Format a string with a color
def format(string, color):
    if os.name == 'posix':
        return '{}{}{}'.format(color.value, string, Color.CLR.value)
    return string

##########################################################################################


def _install_metareserve(location, silent=False):
    if library_exists('metareserve'):
        return True
    if not pip_install(py='python3'):
        return False
    if not os.path.exists(location):
        if subprocess.call('git clone https://github.com/Sebastiaan-Alvarez-Rodriguez/metareserve', shell=True, cwd=location, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) != 0:
            return False
    kwargs = {'shell': True}
    if silent:
        kwargs['stderr'] = subprocess.DEVNULL
        kwargs['stdout'] = subprocess.DEVNULL
    return subprocess.call('pip3 install . --user', cwd=os.path.join(location, 'metareserve'), **kwargs) == 0


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

    if not os.path.exists(location):
        if subprocess.call('git clone https://github.com/ceph/ceph-deploy', shell=True, cwd=location, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL) != 0:
            return False
    kwargs = {'shell': True}
    if silent:
        kwargs['stderr'] = subprocess.DEVNULL
        kwargs['stdout'] = subprocess.DEVNULL
    return subprocess.call('pip3 install . --user', cwd=os.path.join(location, 'ceph-deploy'), **kwargs) == 0


def install_ceph(location, reservation_str, silent=False):
    '''Installs ceph on all nodes. Requires updated package manager.
    Warning: This only has to be executed on 1 node, which will be designated the `ceph admin node`.
    Warning: Expects to find a 'designations' extra-info key, with as value a comma-separated string for each node in the reservation, listing its designations. 
             Daemons for the given designations will be installed.
             E.g. node.extra_info['designations'] = 'mon,mds,osd,osd' will install the monitor, metadata-server and osd daemons.
             Note: Designations may be repeated, without effect.
             Warning: Each node must have at least 1 designation.
    Warning: We assume apt package manager.
    Args:
        location (str): Location to install metareserve in. metareserve root will be`location/metareserve`.
        reservation_str (str): String representation of reservation, containing all nodes of the cluster for which we install RADOS-ceph.
        silent (optional bool): If set, does not print compilation progress, output, etc. Otherwise, all output will be available.
    Returns:
        `True` on success, `False` on failure.'''
    if not _install_metareserve(location, silent):
        return False
    from metareserve import Reservation
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
    if not _install_metareserve(location, silent):
        return False
    from metareserve import Reservation
    reservation = Reservation.from_string(reservation_str)
    home = os.path.expanduser('~/')
    dest = os.path.join(location, 'arrow')

    kwargs = {'shell': True}
    if silent:
        kwargs['stderr'] = subprocess.DEVNULL
        kwargs['stdout'] = subprocess.DEVNULL

    if not os.path.exists('{}/cpp/build/latest'.format(dest)):
        if subprocess.call('sudo apt install libradospp-dev rados-objclass-dev openjdk-8-jdk openjdk-11-jdk libboost-all-dev automake bison flex g++ git libevent-dev libssl-dev libtool make pkg-config maven cmake thrift-compiler -y', **kwargs) != 0:
            return False
        if (not os.path.isdir(dest)) and subprocess.call('git clone https://github.com/Sebastiaan-Alvarez-Rodriguez/arrow.git -b merge_bridge_dev', cwd=location, **kwargs) != 0:
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


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))