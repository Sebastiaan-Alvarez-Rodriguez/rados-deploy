
'''Small file to help with RADOS-Ceph deployment.'''
def get_subprocess_kwargs(silent):
    if silent:
        return {'shell': True, 'stderr': subprocess.DEVNULL, 'stdout': subprocess.DEVNULL}
    return {'shell': True}
