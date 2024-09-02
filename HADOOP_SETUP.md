# Hadoop Cluster Setup
This tree features the following files:
```
├── hadoop_clone_and_build.sh
├── hadoop_install_master.sh
├── hadoop_install_slave.sh
├── hadoop_prerequisites.sh
├── HADOOP_SETUP.md # This file
└── hadoop_ssh.sh
```
The scripts target hand have been verified with Hadoop release `3.4.0`.

## Build
From any node, preferably the designated master: source build script, which also sources `hadoop_prerequisites.sh`.
``` console
$ source hadoop_clone_and_build.sh  
```
This is going to export a `hadoop-x.x.x.tar.gz` archive in the same directory. This file can be used from any node in the following steps.

## Create "hadoop" User
On every node, setup an hadoop user. It does not need to be a sudoer.
``` console
$ sudo useradd -m hadoop
$ sudo passwd hadoop 
```

## Setup SSH 
Login as `hadoop` user on the designated master node. Put `hadoop-x.x.x.tar.gz` in this directory if not already here.
> Assuming SSH server is already running on every node.
When propted with the passphrase, **leave it empty**!
``` console
$ source hadoop_ssh.sh
```

## Install
From master node, source the installation script configuring every node:
``` console
$ source hadoop_install_master.sh
```

Instead, for a single slave node:
``` console
$ source hadoop_install_slave.sh
```

This scripts assume a clean installation on an untainetd `.bashrc`, for subsequent installations, just override HADOOP_HOME with `<your-new-path>` before sourcing it:
``` console
$ sed -E -i "s|HADOOP_HOME:=.+|HADOOP_HOME=<your-new-path>|g" hadoop_install.sh
```

## Mount Disks
For each node, mount disks for HDFS storage. Actually, link to available mount points for convenience.
``` console
$ ln -s /disk1 /home/hadoop/hadoop_storage/disk1
$ ln -s /disk2 /home/hadoop/hadoop_storage/disk2 
```
If you wish to change mount points:
``` console
# Make and own directories
$ mkdir-p /home/hadoop/hadoop_storage/disk1
$ mkdir-p /home/hadoop/hadoop_storage/disk2 
# Mount disks
umount /dev/sda
sudo mkfs -t ext4 /dev/sda
sudo mount /dev/sda /home/hadoop/hadoop_storage/sda
umount /dev/sdb
sudo mkfs -t ext4 /dev/sdb
sudo mount /dev/sdb /home/hadoop/hadoop_storage/sdb
```

# Operations on HDFS
Before starting HDFS the first time, you need to format it (from any node):
``` console
$ hdfs namenode -format
```

## Start/Stop Deamons
In your $PATH, start-up and stop scripts are available from the Hadoop installation, which can be directly executed as:
```
start-dfs.sh
start-yarn.sh
stop-dfs.sh
stop-yarn.sh
```

## Troubleshooting
In case of faults on a node, try solutions in the follwing.

### Can't Access Web Interfaces
Configure firewall:
``` console
# firewall-cmd --add-port=9870/tcp  # for NameNode
# firewall-cmd --add-port=8088/tcp  # for YARN
# firewall-cmd --runtime-to-permanent
```
> Requires sudo

### DataNodes/NodesManagers can't Access master
Configure firewall:
``` console
# firewall-cmd --add-port=9000/tcp  # for NameNode
# firewall-cmd --add-port=8031/tcp  # for ResourceManader
# firewall-cmd --runtime-to-permanent
```
Make sure DNS is configured for "master". Check `/etc/hosts`.

### DataNodes can't be accessed by master
Configure firewall on DataNodes:
``` console
# firewall-cmd --add-port=9866/tcp  # for Master
# firewall-cmd --runtime-to-permanent
```
Make sure DNS is configured for "master". Check `/etc/hosts`.

### Disable Firewall
For each node:
> NOTE: this is not meant for production!
``` console
# systemctl stop firewalld # non-permanent after reboot
# systemctl disable firewalld # permanent after reboot
``` 

### DataNode not starting
Across experiments, DataNodes may not start due to UUIDs mismatches in blocks. In this case, DFS must be cleaned and re-formatted.

On every node, remove the temporary files and HDFS storage directory(ies):
``` console
# On every node
$ rm -rf /tmp/hadoop-*
$ rm -rf /home/hadoop/hadoop_storage/disk1/*
$ rm -rf /home/hadoop/hadoop_storage/disk2/*
```
Then, reformat the DFS (e.g. from master). 
``` console
$ hdfs namenode -format
```
Or use the utility script (e.g. from master):
``` console
$ source hadoop_recovery.sh
``` 

