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
import Gateway
import zfs

# test harnesses
import disktest
import fstest
import servertest
import gatewaytest

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

def test(data, server, gateway, tests):
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
    myScpu = SimCPU.CPU(server['cpu'],
                        speed=server['speed'],
                        cores=server['cores'])
    mySnic = SimIFC.NIC("eth",
                        processor=myScpu,
                        bw=server['nic'])
    myShba = SimIFC.HBA("HBA",
                        processor=myScpu,
                        bw=server['hba'])
    myServer = Server.Server(myData, num_disks=server['disks'],
                             cpu=myScpu, num_cpus=server['cpus'],
                             nic=mySnic, num_nics=server['nics'],
                             hba=myShba, num_hbas=server['hbas'])


    # instantiate the distributed system
    myGcpu = SimCPU.CPU(gateway['cpu'],
                        speed=gateway['speed'],
                        cores=gateway['cores'])
    myGfront = SimIFC.NIC("eth",
                        processor=myGcpu,
                        bw=gateway['front'])
    myGback = SimIFC.NIC("eth",
                        processor=myGcpu,
                        bw=gateway['back'])
    myGate = Gateway.Gateway(myServer, num_servers=gateway['servers'],
                             cpu=myScpu, num_cpus=gateway['cpus'],
                             front_nic=myGfront, num_front=gateway['fronts'],
                             back_nic=myGback, num_back=gateway['backs'],
                             n=gateway['n'], m=gateway['m'],
                             stripe=gateway['stripe'])

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
            if 'FioRbs' in tests:
                bs = tests['FioRbs']
                disktest.tptest(myData.disk, filesize=sz, depth=d, bsizes=bs)
            else:
                disktest.tptest(myData.disk, filesize=sz, depth=d)
            print("")

    # fio to the data file system
    sz = tests['FioFsize']
    if 'FioFdepths' in tests:
        for d in tests['FioFdepths']:
            print("FIO (direct) to %s, depth=%d" % (data_desc, d))
            if 'FioFbs' in tests:
                bs = tests['FioFbs']
                fstest.fstest(myServer.data_fs, filesize=sz, depth=d,
                              direct=True, bsizes=bs)
            else:
                fstest.fstest(myServer.data_fs, filesize=sz, depth=d,
                              direct=True)
            print("")

    # server throughput tests
    if 'SioSdepths' in tests:
        msg = "server-throughput: %dx%s, %dx%s, %dx%s, %dx%s, depth=%d"
        for d in tests['SioSdepths']:
            print(msg % (
                  myServer.num_cpus, myServer.cpu.desc,
                  myServer.num_disks, data_desc,
                  myServer.num_nics, myServer.nic.desc,
                  myServer.num_hbas, myServer.hba.desc,
                  d))
            if 'SioSbs' in tests:
                bs = tests['SioSbs']
                servertest.servertest(myServer, depth=d, bsizes=bs)
            else:
                servertest.servertest(myServer, depth=d)
            print("")

    if 'SioCdepths' in tests:
        msg = "gateway throughput: %dx%s, front=%dx%s, back=%dx%s, depth=%d"
        for d in tests['SioCdepths']:
            print(msg % (
                  myGate.num_cpus, myGate.cpu.desc,
                  myGate.num_fronts, myGate.front.desc,
                  myGate.num_backs, myGate.back.desc,
                  d))
            if 'SioCbs' in tests:
                bs = tests['SioCbs']
                gatewaytest.gatewaytest(myGate, depth=d, bsizes=bs)
            else:
                gatewaytest.gatewaytest(myGate, depth=d)
            print("")

    # check for warnings
    if myServer.warnings != "" or myGate.warnings != "":
        print("WARNINGS:")
    if myServer.warnings != "":
        print(myServer.warnings)
    if myGate.warnings != "":
        print(myGate.warnings)


#
# standard test parameters and usage example
#
if __name__ == '__main__':

    data = {        # data storage devices
        'device': "disk",
        'fs': "zfs"
    }

    gateway = {     # gateway
        'cpu': "xeon",
        'speed': 2.2 * GIG,
        'cores': 2,
        'cpus': 1,
        'front': 10 * GIG,
        'fronts': 1,
        'back': 10 * GIG,
        'backs': 1,
        'n': 5,
        'm': 2,
        'stripe': 128 * KB,
        'servers': 4,
    }

    server = {      # file server
        'cpu': "xeon",
        'speed': 2.5 * GIG,
        'cores': 2,
        'cpus': 1,
        'nic': 10 * GIG,
        'nics': 1,
        'hba': 8 * GIG,
        'hbas': 1,
        'disks': 4,
    }

    tests = {       # what tests to run with what parameters
        # raw disk parameters and simulations
        'DiskParms': True,
        'FioRdepths': [1, 32],
        'FioRsize': 16 * GIG,
        'FioRbs': (4096, 128 * 1024, 4096 * 1024),

        # FS performance tests
        'FioJournal': True,
        'FioFdepths': [1, 32],
        'FioFsize': 16 * GIG,
        'FioFbs': (4096, 128 * 1024, 4096 * 1024),

        # Server performance tests
        'SioSdepths': [1,16],
        'SioSbs': (4096, 128 * 1024, 4096 * 1024),

        # Gateway performance tests
        'SioCdepths': [1,16],
        'SioCbs': (4096, 128 * 1024, 4096 * 1024),
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
        test(data, server, gateway, notests)
    else:
        test(data, server, gateway, tests)
