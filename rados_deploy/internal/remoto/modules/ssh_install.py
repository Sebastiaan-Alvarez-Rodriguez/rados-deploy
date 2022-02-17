import hashlib
import os
import subprocess


def already_installed(privkey_sha256):
    '''Checks if we already have installed SSH keys on this node. We define 'already installed' as 
    (1) key already exists and (2) key contents are (sha256-)equal to supplied `privkey_sha256`.
    If `privkey_sha256` == `None`, this second condition is ignored.
    Args:
        privkey_sha256 (str): Hexstring of sha256 of private key.

    Returns:
        `True` if the SSH keys are already installed.'''
    home = os.path.expanduser('~')
    keyfile = '{}/.ssh/rados_deploy.rsa'.format(home)
    if not isfile(keyfile):
        return False

    if privkey_sha256 == None:
        return True
    with open(keyfile, 'rb') as f:
        byte_buf = f.read() # read entire file as bytes. A private key file is only a few KB anyway.
        bytestring = hashlib.sha256(byte_buf).hexdigest()
    return bytestring == privkey_sha256


def install_ssh_keys(hosts, keypair, user, use_sudo=True):
    '''Adds an SSH entry in the SSH config of this node, for each info.
    Warning: This has to be executed on each node node.
    Args:
        hosts (list(str)): List of hostnames forming the cluster.
        keypair (tuple(str, str)): (private, public) key to use when connecting to nodes within the cluster. Sending a private key is a little dodgy, until you think: 
                   This connection is used over SSH, and has equivalent protective  measures as SSL.
                   There is as much risk involved as entering banking credentials on your (SSL-secured) bank site.
                   Still, more security is always a bonus.
                   Note that this function is only called once per cluster for a new private/public keypair.
                   If you know a better way to make n-to-n SSH communication possible, make a pull request.
        user (str): Username (must be the same for each node).
        use_sudo (optional bool): If set, also installs SSH keys for the root user.

    Returns:
        `True` on success, `False` on failure.'''
    home = os.path.expanduser('~')

    mkdir('{}/.ssh'.format(home), exist_ok=True)
    if isfile('{}/.ssh/config'.format(home)):
        with open('{}/.ssh/config'.format(home)) as f:
            hosts_available = [line[5:].strip().lower() for line in f.readlines() if line.startswith('Host ')]
    else:
        hosts_available = []
    neededinfo = sorted(hosts)

    local_ip = ''.join('''{0} {1}\n'''.format(x[3:].replace("-", "."), x) for x in neededinfo)
    with open('{}/hosts'.format(home), mode='a') as f:
        # f.write('127.0.0.1 localhost\n')
        f.write(local_ip)
    subprocess.call('sudo cp {}/hosts /etc/hosts'.format(home),shell=True)
    # subprocess.call('sudo cat {}/hosts >> /etc/hosts'.format(home),shell=True)

    # if subprocess.call('sudo vgcreate --force --yes "ceph" "/dev/nvme1n1"',shell=True) != 0:
    #     return False
    # if subprocess.call('sudo lvcreate --yes -l 40%VG -n "ceph-lv-1" "ceph"',shell=True) != 0:
    #     return False
    # if subprocess.call('sudo lvcreate --yes -l 40%VG -n "ceph-lv-2" "ceph"',shell=True) != 0:
    #     return False

    config = ''.join('''
Host {0}
    Hostname {0}
    User {1}
    IdentityFile {2}/.ssh/rados_deploy.rsa
    StrictHostKeyChecking no
    IdentitiesOnly yes
'''.format(x, user, home) for x in neededinfo)
    with open('{}/.ssh/config'.format(home), 'a') as f:
        f.write(config)

    with open('{}/.ssh/authorized_keys'.format(home), 'r') as f:
        pubkey = keypair[1].strip()
        key_authorized = any(1 for x in f.readlines() if x.strip() == pubkey)

    if not key_authorized:
        with open('{}/.ssh/authorized_keys'.format(home), 'a') as f:
            f.write('\n{}\n'.format(pubkey))

    with open('{}/.ssh/rados_deploy.rsa'.format(home), 'w') as f:
        f.write(keypair[0])
    with open('{}/.ssh/rados_deploy.rsa.pub'.format(home), 'w') as f:
        f.write(keypair[1])
    os.chmod('{}/.ssh/rados_deploy.rsa'.format(home), 0o600)

    return subprocess.call('sudo cp {}/.ssh/config /root/.ssh/'.format(home), shell=True) == 0 if use_sudo else True