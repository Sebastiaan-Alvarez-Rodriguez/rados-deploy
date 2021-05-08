# rados-deploy
Framework and CLI-tool to install RADOS-Ceph clusters on remote clusters. 

## Requirements
 - Python>=3.2
 - remoto>=1.2.0
 - metareserve>=0.1.0

Also, there are several requirements for the RADOS-Ceph cluster-to-be:
 1. There must be a local network between all nodes.
 2. SSH must be available between all nodes, on port 22.
 3. The local nodes must have an internet connection.
 4. Superuser rights must be available on all nodes.
 5. The 'admin' node must be accessible from the machine you run this program on, using SSH.


## Installing
Simply execute `pip3 install . --user` in the root directory.


## Usage
Once this project is installed, a `rados-deploy` CLI program becomes available.
It can perform several commands:
 1. `rados-deploy install` allows us to install RADOS-Ceph on remote nodes.
 2. `rados-deploy start/stop/restart` allos us to start/stop/restart RADOS-Ceph on remote nodes.


For more information, optional arguments etc use:
```bash
rados-deploy -h
```

### Reservation Strings
Each command asks the user to provide the reservation string of the cluster to work with.
When using [metareserve](https://github.com/Sebastiaan-Alvarez-Rodriguez/metareserve) to allocate nodes, such a string will be printed.
It looks something like:
```
id,hostname,ip_local,ip_public,port,extra_info
```
```
0|node0|192.168.1.1|100.101.102.200|22|user=Username
1|node1|192.168.1.2|100.101.102.210|22|user=Username
2|node2|192.168.1.3|100.101.102.207|22|user=Username
3|node3|192.168.1.4|100.101.102.208|22|user=Username
4|node4|192.168.1.5|100.101.102.213|22|user=Username
5|node5|192.168.1.6|100.101.102.211|22|user=Username
6|node6|192.168.1.7|100.101.102.209|22|user=Username
7|node7|192.168.1.8|100.101.102.212|22|user=Username
8|node8|192.168.1.9|100.101.102.254|22|user=Username
9|node9|192.168.1.10|100.101.102.200|22|user=Username
```
The `user` field in the `extra_info` is used to connect to the clusters.

This program needs to know what kind of Ceph daemon must be spawned on which node. For that, we use a `designations` field.
An example:
```
0|node0|192.168.1.1|128.110.153.164|22|user=Sebas|designations=mon,mgr,osd,mds
1|node1|192.168.1.2|128.110.153.162|22|user=Sebas|designations=mon,mgr,osd,mds
2|node2|192.168.1.3|128.110.153.190|22|user=Sebas|designations=mon,mgr,osd,osd,osd,mds
3|node3|192.168.1.4|128.110.153.167|22|user=Sebas
4|node4|192.168.1.5|128.110.153.174|22|user=Sebas
5|node5|192.168.1.6|128.110.153.186|22|user=Sebas
6|node6|192.168.1.7|128.110.153.177|22|user=Sebas
7|node7|192.168.1.8|128.110.153.160|22|user=Sebas
8|node8|192.168.1.9|128.110.153.184|22|user=Sebas
9|node9|192.168.1.10|128.110.153.195|22|user=Sebas
```
Here, only node0, node1, node2 will host Ceph daemons. Each one will host a monitor (`mon`), manager (`mgr`), object store device (`osd`), metadata server (`mds`).

When specifying `osd` designation `X` times for a node (e.g. 3 times for node2 in the example), the program spawns `X` `osds` on that node.
This will not work for other designations, as Ceph is not able to host multiple daemons of the other types on the same machine.

> **Note**: Ceph documentation requires 3 `osds`, 3 `mons`, 2 `mgrs`, 2 `mdss` to be available as a minimum.

> **Note**: Ceph documentation states that 1 `osd` takes all the bandwidth of a harddisk under load. The docs recommend only hosting 2 `osds` on a node if it uses a  NVME storage device with high bandwidth. 

All nodes that have no designations will not partake in the Ceph cluster. Each one of them will get a CephFS mountpoint, however.


## Project status
Normally, Ceph is able to host several storage systems. Currently, we support:
 - `memstore`, a system storing data in RAM. Note that stopping or restarting these types of Ceph clusters will delete all data.