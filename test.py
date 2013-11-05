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
import Dlm
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


def makeServer(fs, dict):
    """ instantiate the server node described by a configuration dict
        fs -- file system on which data is stored
        dict -- of server parameters
    """

    # FIX add default number of disks
    numSdisk = dict['disks']

    # FIX add default CPU and number
    myScpu = SimCPU.CPU(dict['cpu'],
                        speed=dict['speed'],
                        cores=dict['cores'])
    numScpu = dict['cpus']

    # FIX add default NIC and number
    mySnic = SimIFC.NIC("eth",
                        processor=myScpu,
                        bw=dict['nic'])
    numSnic = dict['nics']

    # FIX add default HBA and number
    myShba = SimIFC.HBA("HBA",
                        processor=myScpu,
                        bw=dict['hba'])
    numShba = dict['hbas']

    return Server.Server(fs, num_disks=numSdisk,
                         cpu=myScpu, num_cpus=numScpu,
                         nic=mySnic, num_nics=numSnic,
                         hba=myShba, num_hbas=numShba)


def makeDLM(dict):
        # FIX add default CPU and number
    mycpu = SimCPU.CPU(dict['cpu'],
                       speed=dict['speed'],
                       cores=dict['cores'])
    numcpu = dict['cpus']

    # FIX add default NIC and number
    mynic = SimIFC.NIC("eth",
                       processor=mycpu,
                       bw=dict['nic'])
    numnic = dict['nics']

    return Dlm.DLM(cpu=mycpu, num_cpus=numcpu,
                   nic=mynic, num_nics=numnic)


def makeGateway(server, dlm, dict):

    # FIX add default number of servers
    numSvr = dict['servers']

    # FIX add default CPU and number
    myGcpu = SimCPU.CPU(dict['cpu'],
                        speed=dict['speed'],
                        cores=dict['cores'])
    numGcpu = dict['cpus']

    # FIX add default front-side NIC and number
    myGfront = SimIFC.NIC("eth",
                          processor=myGcpu,
                          bw=dict['front'])
    numGfront = dict['fronts']

    # FIX add default front-side NIC and number
    myGback = SimIFC.NIC("eth",
                         processor=myGcpu,
                         bw=dict['back'])
    numGback = dict['backs']

    # FIX add default stripe parameters
    n = dict['n']
    m = dict['m']
    strip = dict['strip']

    return Gateway.Gateway(server, dlm,
                           num_servers=numSvr,
                           cpu=myGcpu, num_cpus=numGcpu,
                           front_nic=myGfront, num_front=numGfront,
                           back_nic=myGback, num_back=numGback,
                           n=n, m=m, strip=strip)


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

    # instantiate a data server
    myServer = makeServer(myData, server)

    # instantiate a DLM
    myDLM = makeDLM(dlm)

    # instantiate a gateway server
    myGate = makeGateway(myServer, myDLM, gateway)

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
                              direct=True, bsizes=bs, crtdlt=False)
            else:
                fstest.fstest(myServer.data_fs, filesize=sz, depth=d,
                              direct=True, crtdlt=False)
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

    data = {
        # data storage devices
        'device': "disk",
        'fs': "zfs"
    }

    gateway = {
        # gateway
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
        'strip': 128 * KB,
        'servers': 4
    }

    server = {
        # file server
        'cpu': "xeon",
        'speed': 2.5 * GIG,
        'cores': 2,
        'cpus': 1,
        'nic': 10 * GIG,
        'nics': 1,
        'hba': 8 * GIG,
        'hbas': 1,
        'disks': 4
    }

    dlm = {
        # Distributed Lock Manager
        'cpu': "xeon",
        'speed': 2.5 * GIG,
        'cores': 2,
        'cpus': 1,
        'nic': 10 * GIG,
        'nics': 1
    }

    tests = {
        # what tests to run with what parameters

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
        'SioSdepths': [1, 16],
        'SioSbs': (4096, 128 * 1024, 4096 * 1024),

        # Gateway performance tests
        'SioCdepths': [1, 16],
        'SioCbs': (4096, 128 * 1024, 4096 * 1024)
    }

    notests = {
        # just generate simulation data
        'FioFsize': 16 * GIG,
        'perfdata': True
    }

    from optparse import OptionParser
    parser = OptionParser("usage: %prog [options]")
    parser.add_option("-d", "--data", dest="sim", action="store_true",
                      default=False,
                      help="produce simulated FS performance data")
    (opts, files) = parser.parse_args()
    if opts.sim:
        test(data, server, gateway, notests)
    else:
        test(data, server, gateway, tests)
