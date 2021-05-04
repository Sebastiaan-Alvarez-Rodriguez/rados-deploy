import os
import subprocess

from metareserve import Reservation

import internal.util.fs as fs

def install_ssh_keys(reservation_str, keypair, user, use_sudo=True):
    '''Adds an SSH entry in the SSH config of this node, for each info.
    Warning: This has to be executed on each node node.
    Args:
        reservation_str (str): String representation of reservation, containing all nodes of the cluster for which we install RADOS-ceph.
        keypair (tuple(str, str)): (private, public) key to use when connecting to nodes within the cluster. Sending a private key is a little dodgy, until you think: 
                   This connection is used over SSH, and has equivalent protective  measures as SSL.
                   There is as much risk involved as entering banking credentials on your (SSL-secured) bank site.
                   Still, more security is always a bonus.
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

    with open('{}/.ssh/rados_deploy.rsa'.format(home), 'w') as f:
        f.write(keypair[0])
    
    with open('{}/.ssh/authorized_keys'.format(home), 'r') as f:
        pubkey = keypair[1].strip()
        key_authorized = any(1 for x in f.readlines() if x.strip() == pubkey)

    if not key_authorized:
        with open('{}/.ssh/authorized_keys'.format(home), 'a') as f:
            f.write('\n{}\n'.format(pubkey))

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


if __name__ == '__channelexec__': # In case we use this module with remoto legacy connections (local, ssh), we need this footer.
    for item in channel:
        channel.send(eval(item))