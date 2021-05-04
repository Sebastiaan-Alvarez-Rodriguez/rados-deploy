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
    home = os.path.expanduser('~/')
    keyfile = '{}/.ssh/rados_deploy.rsa'.format(home)
    if not os.path.isfile(keyfile):
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
    home = os.path.expanduser('~/')

    os.makedirs('{}/.ssh'.format(home), exist_ok=True)
    if os.path.isfile('{}/.ssh/config'.format(home)):
        with open('{}/.ssh/config'.format(home)) as f:
            hosts_available = [line[5:].strip().lower() for line in f.readlines() if line.startswith('Host ')]
    else:
        hosts_available = []
    neededinfo = sorted(hosts)

    config = ''.join('''
Host {0}
    Hostname {0}
    User {1}
    IdentityFile {2}/.ssh/rados_deploy.rsa
    StrictHostKeyChecking accept-new
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

    return subprocess.call('sudo cp {}/.ssh/config /root/.ssh/'.format(home), shell=True) == 0 if use_sudo else True


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))