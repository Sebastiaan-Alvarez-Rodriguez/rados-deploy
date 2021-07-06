import importlib
import os
import subprocess
import sys
import tempfile
import urllib.request


'''Functions to interact with Python's import libraries. As the import libraries change a lot between versions, this file is essential to work with importlib.'''


def library_exists(name):
    '''Check if a given library exists. Returns True if given name is a library, False otherwise.'''
    if sys.version_info >= (3, 6):
        import importlib.util
        return importlib.util.find_spec(str(name)) is not None
    if sys.version_info >= (3, 4):
        return importlib.util.find_spec(str(name)) is not None
    else:
        raise NotImplementedError('Did not implement existence check for Python 3.3 and below')

def import_full_path(full_path):
    '''Import a library from a filesystem full path (i.e. starting from root)
    Returns:
        Imported module.'''
    module_name = '.'.join(full_path.split(os.path.sep))
    if sys.version_info >= (3, 6):
        import importlib.util
        spec = importlib.util.spec_from_file_location(module_name, full_path)
        foo = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(foo)
        return foo
    elif sys.version_info >= (3, 5):
        import importlib.util
        spec = importlib.util.spec_from_file_location(module_name, full_path)
        foo = importlib.util.module_from_spec(spec)
        return spec.loader.exec_module(foo)
    elif sys.version_info >= (3, 3):
        from importlib.machinery import SourceFileLoader
        return SourceFileLoader(module_name, full_path).load_module()
    elif sys.version_info <= (2, 9):
        import imp
        return imp.load_source(module_name, full_path)
    else:
        raise NotImplementedError('Did not implement existence check for Python >2.9 and <3.3')

def __pip_installed(pip, silent=False):
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {}
    return subprocess.call('{} -h'.format(pip), shell=True, **kwargs) == 0

def __pip_install0(py, silent=False):
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {}
    return subprocess.call('{} -m ensurepip'.format(py), shell=True, **kwargs) == 0

def __pip_install1(py, silent=False):
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {}
    if subprocess.call('sudo apt update -y', shell=True, **kwargs) != 0:
        return False
    return subprocess.call('sudo apt install -y {}-pip'.format(py), shell=True, **kwargs) == 0 #, stderr=subprocess.DEVNULL, stdout=subprocess.DEVNULL

def __pip_install2(py, silent=False):
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
                    if not silent:
                        printw('Could not download get-pip. Retrying...')
                elif x == retries-1:
                    if not silent:
                        printe('Could not download get-pip: {}'.format(e))
                    return False

        kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {}
        return subprocess.call('{} {}'.format(py, archiveloc), shell=True, **kwargs) == 0


def pip_install(py='python3', pip='pip3', silent=False):
    '''Installs pip.
    Args:
        py (optional str): Python executable. Some systems use 'python' for python2, 'python3' for 'python3', but this is not always the case.
        pip (optional str): Python-pip executable. Some systems use `pip3` for python3-pip, others use `pip`.

    Returns:
        `True` on success, `False` on failure.'''
    return __pip_installed(pip, silent) or __pip_install0(py, silent) or __pip_install1(py, silent) or __pip_install2(py, silent)


def lib_install(name, usermode=False, py='python3', pip='pip3', silent=False):
    '''Installs library using pip.
    Args:
        name (str): Name of package/library to install.
        usermode (optional bool): If set, installs in usermode ('--user' tag), globally otherwise.
        py (optional str): Python executable. Some systems use 'python' for python2, 'python3' for 'python3', but this is not always the case.
        pip (optional str): Python-pip executable. Some systems use `pip3` for python3-pip, others use `pip`.
        silent (optional bool): If set, prints less verbose output.

    Returns:
        `True` on success, `False` on failure.'''
    if not pip_install(py, pip, silent=silent):
        return False
    cmd = 'pip3 install {}'.format(name)
    if usermode:
        cmd += ' --user'
    kwargs = {'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL} if silent else {}
    return subprocess.call(cmd, shell=True, **kwargs) == 0