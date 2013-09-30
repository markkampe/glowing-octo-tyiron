#!/usr/bin/python
#
# nonesuch
#

"""
This module creates test objects and kicks off a standard set of tests.

The primary entry point (test) is driven by a set of configuration
dictionaries, describing the devices, cluster, and tests to be run
"""

# simulations
import SimDisk
import SimFS
import SimIFC
import SimCPU
import Server
import Cluster
import zfs

# test harnesses
import disktest
import fstest
import servertest
import clustertest

from units import *


def makedisk(dict):
    """ instantiate the disk described by a configuration dict
            device -- type of device to create (default disk)
            size -- usable space (default 2TB)
            rpm -- rotational speed (default 7200 RPM)
            speed -- max transfer speed (default 150MB/s)
            iops -- max iops
            heads -- number of heads
            streams -- max concurrent streams
            shared -- (for journals, do multiple OSDs share this device)
    """

    # collect universal parameters
    sz = dict['size'] if 'size' in dict else 2 * TERA
    spd = dict['speed'] if 'speed' in dict else 150 * MEG

    if 'device' in dict and dict['device'] == 'ssd':
        iops = dict['iops'] if 'iops' in dict else 20000
        strm = dict['streams'] if 'streams' in dict else 1
        return SimDisk.SSD(sz, spd, iops=iops, streams=strm)
    elif 'device' in dict and dict['device'] == 'dumb':
        rpm = dict['rpm'] if 'rpm' in dict else 7200
        heads = dict['heads'] if 'heads' in dict else 10
        return SimDisk.DumbDisk(rpm, sz, spd, heads=heads)
    else:
        rpm = dict['rpm'] if 'rpm' in dict else 7200
        heads = dict['heads'] if 'heads' in dict else 10
        return SimDisk.Disk(rpm, sz, spd, heads=heads)


def makefs(disk, dict):
    """ instantiate the filesystem described by a configuration dict
        disk -- on which file system is to be created
        dict -- of file system paramters
             -- fs: type of file system
             -- age: 0-1
    """

    age = dict['age'] if 'age' in dict else 0

    if 'fs' in dict and dict['fs'] == 'btrfs':
        return SimFS.btrfs(disk, age)
    elif 'fs' in dict and dict['fs'] == 'ext4':
        return SimFS.ext4(disk, age)
    elif 'fs' in dict and dict['fs'] == 'zfs':
        return zfs.zfs(disk, age)
    elif 'fs' in dict and dict['fs'] == 'xfs':
        return SimFS.xfs(disk, age)
    else:
        return SimFS.xfs(disk, age)


def makecluster(server, dict):
    """ instantiate the cluster described by a configuration dict
        blockserver -- on which cluster is based
        dict -- of cluster configuration parameters
             -- nodes: number of nodes in cluster
             -- disk_per_node: number of OSDs per node
             -- front: speed of front-side network
             -- back: speed of back-side network
    """

    return Cluster.Cluster(server,
            front_nic=dict['front'],
            back_nic=dict['back'],
            nodes=dict['nodes'],
            disk_per_node=dict['disk_per_node'])


def test(data, journal, cluster, tests):
    """ run a specific set of tests on a specific cluster simulation
        data -- dictionary describing the data devices
        journal -- dictionary describing the journal devices
        cluster -- dictionary describing the cluster
        tests -- dictionary describing the tests to be run
    """

    # instantiate the data device simulation
    myDDisk = makedisk(data)
    myData = makefs(myDDisk, data)
    data_fstype = myData.desc
    data_dev = myDDisk.desc
    data_desc = "%s (on %s)" % (data_fstype, data_dev)

    # instantiate a server node
    myCPU = SimCPU.CPU("Xeon", speed=3.3 * GIG)
    myNIC = SimIFC.NIC("NIC", processor=myCPU)
    myServer = Server.Server(myData, num_disks=cluster['disk_per_node'],
                             cpu=myCPU, nic=myNIC)

    # instantiate the distributed system
    myClust = makecluster(myServer, cluster)

    #
    # run the specified tests for the specified ranges
    #

    if 'DiskParms' in tests and tests['DiskParms']:
        print("Data Device Characteristics")
        disktest.disktest(myData.disk)
        print("")

    # fio to the raw disk
    if 'FioRsize' in tests and 'FioRdepths' in tests:
        sz = tests['FioRsize']
        for d in tests['FioRdepths']:
            print("Raw data device (%s), depth=%d" % (data_dev, d))
            disktest.tptest(myData.disk, filesize=sz, depth=d)
            print("")

    # fio to the data file system
    sz = tests['FioFsize']
    if 'FioFdepths' in tests:
        for d in tests['FioFdepths']:
            print("FIO (direct) to %s, depth=%d" % (data_desc, d))
            fstest.fstest(myServer.data_fs, filesize=sz, depth=d, direct=True)
            print("")

    # FIX - lose SioSnobj
    # server throughput tests
    if 'SioSsize' in tests:
        msg = "server-throughput: %dx%s, %dx%s, %dx%s, depth=%d"
        sz = tests['SioSsize']      # FIX keep/change?
        for d in tests['SioSdepths']:
            print(msg % (
                  myServer.num_cpus, myServer.cpu.desc,
                  myServer.num_disks, data_desc,
                  myServer.num_nics, myServer.nic.desc, d))
            servertest.servertest(myServer, obj_size=sz, depth=d)
            print("")

        msg = "cluster-throughput (%dx%d), %d copy, "
        msg += "clients*instances*depth=(%d*%d*%d)"
        sz = tests['SioCsize']
        no = tests['SioCnobj']
        for x in tests['SioCcopies']:
            for c in tests['SioCclients']:
                for i in tests['SioCinstances']:
                    for d in tests['SioCdepths']:
                        print(msg %
                            (myClust.num_nodes, myClust.disk_per_node,
                            x, c, i, d))
                        print("\t%s, %s%s, nobj=%d, objsize=%d" %
                                (data_desc, jrnl_desc,
                                "" if j_share == 1 else "/%d" % (j_share),
                                no, sz))
                        clustertest.clustertest(myClust, obj_size=sz, nobj=no,
                                            clients=c, depth=i * d, copies=x)
                        print("")

    # check for warnings
    if myServer.warnings != "" or myClust.warnings != "":
        print("WARNINGS: %s%s" % (myServer.warnings, myClust.warnings))


#
# standard test parameters and usage example
#
if __name__ == '__main__':

    data = {        # data storage devices
        'device': "disk",
        'fs': "zfs"
    }

    journal = {     # journal devices
        'device': "ssd",
        'size': 1 * GIG,
        'speed': 400 * MEG,
        'iops': 30000,
        'streams': 8,
        'fs': "xfs",
        'shared': True
    }

    cluster = {     # cluster configuration
        'front': 1 * GIG,
        'back': 10 * GIG,
        'nodes': 4,
        'disk_per_node': 4
    }

    tests = {       # what tests to run with what parameters
        # raw disk parameters and simulations
        'DiskParms': True,
        'FioRdepths': [1, 32],
        'FioRsize': 16 * GIG,

        # FS performance tests
        'FioJournal': True,
        'FioFdepths': [1, 32],
        'FioFsize': 16 * GIG,

        # Server performance tests
        'SioSdepths': [16],
        'SioSsize': 1 * GIG,

        # Cluster performance tests
        'SioCdepths': [16],
        'SioCsize': 1 * GIG,
        'SioCnobj': 2500 * 4 * 4,   # multiply by number of OSDs
        'SioCcopies': [2],
        'SioCclients': [3],
        'SioCinstances': [4]
    }

    notests = {     # just generate simulation data

        'FioFsize': 16 * GIG,
        'perfdata': True
    }

    from optparse import OptionParser
    parser = OptionParser("usage: %prog [options]")
    parser.add_option("-d", "--data", dest="sim", action="store_true",
                default=False, help="produce simulated FS performance data")
    (opts, files) = parser.parse_args()
    if opts.sim:
        test(data, journal, cluster, notests)
    else:
        test(data, journal, cluster, tests)
