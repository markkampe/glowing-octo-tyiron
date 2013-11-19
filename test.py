#!/usr/bin/python
#
# nonesuch
#

from units import *

#
# configuration for components to be simulated
#
data = {
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

#
# tests to be run
#
tests = {
    # raw disk parameters and simulations
    'DiskParms': True,
    'FioRdepth': [1, 32],
    'FioRsize': 16 * GIG,
    'FioRbs': (4096, 128 * 1024, 4096 * 1024),

    # FIO performance tests
    'FioFdepth': [1, 32],
    'FioFsize': 16 * GIG,
    'FioFbs': (4096, 128 * 1024, 4096 * 1024),
    'Fmisc': False,

    # S erver performance tests
    'SioSdepth': [1, 16],
    'SioSbs': (4096, 128 * 1024, 4096 * 1024),

    # Gateway performance tests
    'SioCdepth': [1, 16],
    'SioCbs': (4096, 128 * 1024, 4096 * 1024),
}

#   instantiate the described objects and run the described tests
#
if __name__ == '__main__':

    from optparse import OptionParser
    parser = OptionParser("usage: %prog [options]")
    parser.add_option("-d", "--data", dest="sim", action="store_true",
                      default=False,
                      help="produce simulated FS performance data")
    (opts, files) = parser.parse_args()

    # instantiate the data device
    import SimDisk
    myDDisk = SimDisk.makedisk(data)
    if 'DiskParms' in tests and tests['DiskParms']:
        print("Data Device Characteristics")
        SimDisk.diskparms(myDDisk)

    # fio to the raw data devices
    SimDisk.tptest(myDDisk, tests, descr="Raw data device")

    # instantiate and test the data file system
    import SimFS
    myData = SimFS.makefs(myDDisk, data)
    data_desc = "%s (on %s)" % (myData.desc, myDDisk.desc)
    SimFS.fstest(myData, tests, descr=data_desc)

    # instantiate and test the data server
    import Server
    myServer = Server.makeServer(myData, server)
    msg = "%dx%s, %dx%s, %dx%s, %dx%s" % (
        myServer.num_cpus, myServer.cpu.desc,
        myServer.num_disks, data_desc,
        myServer.num_nics, myServer.nic.desc,
        myServer.num_hbas, myServer.hba.desc)
    Server.servertest(myServer, tests, descr=msg)

    # instantiate a DLM
    import Dlm
    myDlm = Dlm.makeDLM(dlm)

    # instantiate and test the gateway server
    import Gateway
    myGate = Gateway.makeGateway(myServer, myDlm, gateway)
    msg = "%dx%s, front=%dx%s, back=%dx%s" % (
        myGate.num_cpus, myGate.cpu.desc,
        myGate.num_fronts, myGate.front.desc,
        myGate.num_backs, myGate.back.desc)
    Gateway.gatewaytest(myGate, tests, descr=msg)

    # check for warnings
    if myServer.warnings != "" or myGate.warnings != "":
        print("WARNINGS:")
    if myServer.warnings != "":
        print(myServer.warnings)
    if myGate.warnings != "":
        print(myGate.warnings)

#    if opts.sim:
#        test(data, journal, cluster, notests)
#    else:
#        test(data, journal, cluster, tests)
#    # just generate simulation data
#    notests = {
#        'FioFsize': 16 * GIG,
#        'perfdata': True
#    }
#
#def sample(name, fs, sz):
#    """ collect performance sample data for simulated file system
#        name -- name of the sample
#        fs -- file system to be sampled
#        sz -- fio file size to be simulated
#    """
#    sizes = {
#        '4k': 4086,
#        '128k': 128 * 1024,
#        '4m': 4 * 1024 * 1024
#    }
#
#    print("%sData = {" % (name))
#    print("    'source': 'sampled %s on %s'," % (fs.desc, fs.disk.desc))
#    for d in (1, 32):
#        for b in ("4k", "128k", "4m"):
#            bs = sizes[b]
#            tsr = fs.read(bs, sz, seq=True, depth=d, direct=True)
#            print("    'seq-read-%s-d%d': %d," % (b, d, MEG * bs / tsr))
#            tsw = fs.write(bs, sz, seq=True, depth=d, direct=True)
#            print("    'seq-write-%s-d%d': %d," % (b, d, MEG * bs / tsw))
#            trr = fs.read(bs, sz, seq=False, depth=d, direct=True)
#            print("    'rand-read-%s-d%d': %d," % (b, d, MEG * bs / trr))
#            trw = fs.write(bs, sz, seq=False, depth=d, direct=True)
#            print("    'rand-write-%s-d%d': %d," % (b, d, MEG * bs / trw))
#    print("    }")
#
#    # and capture sampled performance just in case anyone cares
#    if 'perfdata' in tests:
#        print("#")
#        print("# Sampled file system throughput to be fed into DataFS.py to")
#        print("# enable filestore simulation based on actual FS performance")
#        print("#")
#        print("")
#        sample("Data", myData, sz)
#        if myJrnl is not None:
#            print("")
#            sample("Jrnl", myJrnl, sz)
