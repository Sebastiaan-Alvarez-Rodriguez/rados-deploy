import argparse
import concurrent.futures
from concurrent.futures import process
import itertools
from multiprocessing import cpu_count
import os
import subprocess
from tabnanny import check

import data_deploy.shared.copy
import data_deploy.shared.link

import remoto

import rados_deploy.internal.defaults.data as defaults
import rados_deploy.internal.remoto.ssh_wrapper as ssh_wrapper
import rados_deploy.internal.util.fs as fs
import rados_deploy.internal.util.location as loc
from rados_deploy.internal.util.printer import *


'''Deploys data on a running Ceph cluster.'''

def _merge_kwargs(x, y):
    z = x.copy()
    z.update(y)
    return z


def _pick_admin(reservation, admin=None):
    '''Picks an admin node.
    Args:
        reservation (`metareserve.Reservation`): Reservation object to pick admin from.
        admin (optional int): If set, picks node with given `node_id`. Picks node with lowest public ip value, otherwise.

    Returns:
        admin, list of non-admins.'''
    if len(reservation) == 1:
        return next(reservation.nodes), []

    if admin:
        return reservation.get_node(node_id=admin), [x for x in reservation.nodes if x.node_id != admin]
    else:
        tmp = sorted(reservation.nodes, key=lambda x: x.ip_public)
        return tmp[0], tmp[1:]


def _ensure_attr(connection):
    '''Installs the 'attr' package, if not available.'''
    _, _, exitcode = remoto.process.check(connection, 'which setfattr', shell=True)
    if exitcode != 0:
        out, err, exitcode = remoto.process.check(connection, 'sudo apt update && sudo apt install attr -y', shell=True)
        if exitcode != 0:
            printe('Could not install "attr" package (needed for "setfattr" command). Exitcode={}.\nOut={}\nErr={}'.format(exitcode, out, err))
            return False
    with open ('/home/yzhao/aws_credential.txt') as f:
        credential = f.readline()
    access_key_id = credential.split(',')[0]
    secret_access_key = credential.split(',')[1]
    region = credential.split(',')[2]
    _, _, exitcode = remoto.process.check(connection, 'which awscli', shell=True)
    if exitcode != 0:
        out, err, exitcode = remoto.process.check(connection, 'sudo apt install awscli -y && aws configure set aws_access_key_id {} && \
        aws configure set aws_secret_access_key {} && aws configure set default.region {}'.format(access_key_id, secret_access_key, region), shell=True)
        if exitcode != 0:
            printe('Could not install "awscli" package. Exitcode={}.\nOut={}\nErr={}'.format(exitcode, out, err))
            return False
    return True


def _pre_deploy_remote_file(connection, stripe, copies_amount, links_amount, source_file, dest_file):
    remoto.process.check(connection, 'mkdir -p {}'.format(fs.dirname(dest_file)), shell=True)
    _, _, exitcode = remoto.process.check(connection, 'touch {}'.format(dest_file), shell=True)
    if exitcode != 0:
        printe('Could not touch file at cluster: {}'.format(dest_file))
        return False

    if copies_amount > 0 and not data_deploy.shared.copy.copy_single(connection, dest_file, copies_amount, silent=False):
        return False

    if links_amount > 0 and not data_deploy.shared.link.link(connection, expression=data_deploy.shared.copy.copy_expression(dest_file, copies_amount), num_links=links_amount, silent=False):
        return False

    cmd = '''sudo python3 -c "
import itertools
import subprocess
import concurrent.futures
from multiprocessing import cpu_count
with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
    futures_setfattr = [executor.submit(subprocess.call, 'setfattr --no-dereference -n ceph.file.layout.object_size -v {2} {{}}'.format(x), shell=True) for x in itertools.chain(['{0}'], ('{0}.copy.{{}}'.format(x) for x in range({1})))]
    results = [x.result() == 0 for x in futures_setfattr]
exit(0 if all(results) else 1)
"
'''.format(dest_file, copies_amount, stripe*1024*1024)
    out, error, exitcode = remoto.process.check(connection, cmd, shell=True)
    if exitcode != 0:
        printe('Could not stripe file{} at cluster: {}. Is the cluster running?\nReason: Out: {}\n\nError: {}'.format(' (and all {} copies)'.format(copies_amount) if copies_amount > 0 else '', dest_file, '\n'.join(out), '\n'.join(error)))
        return False
    return True


def _post_deploy_remote_file(connection, stripe, copies_amount, links_amount, source_file, dest_file):
    if copies_amount > 0:
        cmd = '''python3 -c "
import subprocess
import concurrent.futures
from multiprocessing import cpu_count
with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
    futures_rsync = [executor.submit(subprocess.call, 'rsync -q -aHAX --inplace {0} {{}}'.format(x), shell=True) for x in ('{0}.copy.{{}}'.format(x) for x in range({1}))]
    results = [x.result() == 0 for x in futures_rsync]
exit(0 if all(results) else 1)
"
'''.format(dest_file, copies_amount)
        _, _, exitcode = remoto.process.check(connection, cmd, shell=True)
        if exitcode != 0:
            printe('Could not inflate dataset using {} copies of file at cluster: {}. Is there enough space?'.format(copies_amount, dest_file))
            return False
    return True


def _execute_internal(connectionwrapper, reservation, paths, dest, silent, copy_multiplier, link_multiplier, admin_node, stripe):
    if not connectionwrapper:
        printe('Could not connect to admin: {}'.format(admin_node))
        return False

    if not _ensure_attr(connectionwrapper.connection):
        return False

    max_filesize = stripe * 1024 * 1024
    copies_to_add = max(1, copy_multiplier) - 1
    links_to_add = max(1, link_multiplier) - 1
    with concurrent.futures.ThreadPoolExecutor(max_workers=cpu_count()-1) as executor:
        files_to_deploy = []
        for path in paths:
            if fs.isfile(path):
                if os.path.getsize(path) > max_filesize:
                    printe('File {} is too large ({} bytes, max allowed is {} bytes)'.format(path, os.path.getsize(path), max_filesize))
                    return False
                files_to_deploy.append((path, fs.join(dest, fs.basename(path))))
            elif fs.isdir(path):
                to_visit = [path]
                path_len = len(path)
                while any(to_visit):
                    visit_now = to_visit.pop()
                    to_visit += list(fs.ls(visit_now, only_dirs=True, full_paths=True))
                    files = list(fs.ls(visit_now, only_files=True, full_paths=True))
                    files_too_big = [x for x in files if os.path.getsize(x) > max_filesize]
                    if any(files_too_big):
                        for x in files_too_big:
                            printe('File {} is too large ({} bytes, max allowed is {} bytes)'.format(x, os.path.getsize(x), max_filesize))
                        return False
                    files_to_deploy += [(x, fs.join(dest, x[path_len+1:])) for x in files]

        if not silent:
            print('Transferring data...')

        futures_pre_deploy = [executor.submit(_pre_deploy_remote_file, connectionwrapper.connection, stripe, copies_to_add, links_to_add, source_file, dest_file) for (source_file, dest_file) in files_to_deploy]
        if not all(x.result() for x in futures_pre_deploy):
            printe('Pre-data deployment error occured.')
            return False
                
        fun = lambda path: subprocess.call('ssh -F {} ubuntu@{} "cd {} && aws s3 sync s3://ceph-dataset/tpcds-1t-split/ ./"'.format(connectionwrapper.ssh_config.name, admin_node.ip_public, fs.join(dest, fs.basename(path))), shell=True) == 0
        futures_rsync = {path: executor.submit(fun, path) for path in paths}
        state_ok = True
        for path,future in futures_rsync.items():
            if not silent:
                print('Waiting on file: {}'.format(path))
            if not future.result():
                state_ok = False
                printe('Could not transfer file: {}'.format(path))
        if not state_ok:
            return False

        # futures_pre_deploy = [executor.submit(_pre_deploy_remote_file, connectionwrapper.connection, stripe, copies_to_add, links_to_add, source_file, dest_file) for (source_file, dest_file) in files_to_deploy]
        # if not all(x.result() for x in futures_pre_deploy):
        #     printe('Pre-data deployment error occured.')
        #     return False

        # Method 1:
        # _, _, exitcode = remoto.process.check(connectionwrapper.connection, 'UNIX Command', shell=True)
        # if exitcode != 0:
        #     return False
        # Method 2:
        # if subprocess.call('ssh -F {} ubuntu@{} "UNIX Command"'.format(connectionwrapper.ssh_config.name, admin_node.ip_public), shell=True) != 0:
        #     return False


        futures_post_deploy = [executor.submit(_post_deploy_remote_file, connectionwrapper.connection, stripe, copies_to_add, links_to_add, source_file, dest_file) for (source_file, dest_file) in files_to_deploy]
        if all(x.result() for x in futures_post_deploy):
            prints('Data deployment success')
            return True
        else:
            printe('Post-data deployment error occured.')
            return False


def description():
    return "Deploys data on a running Ceph cluster."


def origin():
    return "rados-deploy."

def parse(args):
    parser = argparse.ArgumentParser(prog='...')
    parser.add_argument('--admin', metavar='id', dest='admin_id', type=int, default=None, help='ID of the node that will be the primary or admin node.')
    parser.add_argument('--stripe', metavar='amount', type=int, default=defaults.stripe(), help='Striping, in megabytes (default={}MB). Must be a multiple of 4. Make sure that every file is smaller than set stripe size.'.format(defaults.stripe()))
    args = parser.parse_args(args)
    return True, [], {'admin_id': args.admin_id, 'stripe': args.stripe}


def execute(reservation, key_path, paths, dest, silent, copy_multiplier, link_multiplier, *args, **kwargs):
    '''Deploy data on remote RADOS-Ceph clusters, on an existing reservation.
    Dataset sizes can be inflated on the remote, using 2 strategies:
     1. link multiplication: Every dataset file receives `x` hardlinks.
        The hardlinks ensure the dataset size appears to be `x` times larger, but in reality, just the original file consumes space.
        This method is very fast, but has drawbacks: Only the original files are stored by Ceph.
        When using the RADOS-Arrow connector, this means Arrow will spam only the nodes that contain the original data.
        E.g: If we deploy 1 file of 64MB, with link multiplier 1024, the data will apear to be 64GB.
             The storage space used on RADOS-Ceph will still be 64MB, because we have 1 real file of 64MB, and 1023 hardlinks to that 1 file.
             The actual data is only stored on 3 OSDs (with default Ceph Striping factor 3).
             Now, Arrow will spam all work to those 3 OSDs containing the data, while the rest is idle.
     2. file multiplication: Every dataset file receives `x` copies.
        This method is slower than the one listed above, because real data has to be copied. 
        It also actually increases storage usage, contrary to above. 
        However, because we multiply real data, the load is guaranteed to be balanced across nodes, as far as Ceph does that.

    Note that mutiple multiplication techniques can be combined, in which case they stack.
    E.g: If we deploy 1 file of 64MB, with a copy multiplier 4 and a link multiplier 1024, we get 4 real files (1 original + 3 copies),
         and each file gets 1023 hardlinks assigned to it.

    Returns:
        `True` on success, `False` otherwise.'''
    connectionwrapper = kwargs.get('connectionwrapper')
    admin_id = kwargs.get('admin_id')
    stripe = kwargs.get('stripe') or defaults.stripe()

    if stripe < 4:
        raise ValueError('Stripe size must be equal to or greater than 4MB (and a multiple of 4MB)!')
    if stripe % 4 != 0:
        raise ValueError('Stripe size must be a multiple of 4MB!')

    admin_node, _ = _pick_admin(reservation, admin=admin_id)
    use_local_connections = connectionwrapper == None
    if use_local_connections: # We did not get any connections, so we must make them
        ssh_kwargs = {'IdentitiesOnly': 'yes', 'StrictHostKeyChecking': 'no'}
        if key_path:
            ssh_kwargs['IdentityFile'] = key_path

        connectionwrapper = ssh_wrapper.get_wrapper(admin_node, admin_node.ip_public, ssh_params=_merge_kwargs(ssh_kwargs, {'User': admin_node.extra_info['user']}), silent=silent)
    else: # We received connections, need to check if they are valid.
        if not connectionwrapper.open:
            raise ValueError('Provided connection is not open.')

    retval = _execute_internal(connectionwrapper, reservation, paths, dest, silent, copy_multiplier, link_multiplier, admin_node, stripe)
    if not use_local_connections:
        ssh_wrapper.close_wrappers([connectionwrapper])
    return retval