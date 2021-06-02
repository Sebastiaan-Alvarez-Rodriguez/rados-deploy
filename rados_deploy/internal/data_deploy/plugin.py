import rados_deploy.internal.util.fs as fs
import os


def list_plugins():
    '''Returns all paths to files with a name indicating a data-deploy plugin.'''
    return (y for y in fs.ls(fs.dirname(__file__), only_files=True, full_paths=True) if y.endswith('.deploy.plugin.py'))
 

def data_deploy_destination():
    return fs.join(os.path.expanduser('~'), '.data-deploy')
    

def install():
    data_deploy_dst = data_deploy_destination()
    fs.mkdir(data_deploy_dst, exist_ok=True)
    for x in list_plugins():
        dst = fs.join(data_deploy_dst, fs.basename(x))
        if fs.exists(dst) or fs.issymlink(dst):
            print('Found dst (removing): {}'.format(dst))
            fs.rm(dst, ignore_errors=True)
        fs.ln(x, dst)


def remove():
    data_deploy_dst = data_deploy_destination()
    for x in list_plugins():
        dst = fs.join(data_deploy_dst, fs.basename(x))
        fs.rm(dst, ignore_errors=True)