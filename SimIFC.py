#!/usr/bin/python
#
# Ceph - scalable distributed file system
#
# Copyright (C) Inktank
#
# This is free software; you can redistribute it and/or
# modify it under the terms of the GNU Lesser General Public
# License version 2.1, as published by the Free Software
# Foundation.  See file COPYING.
#

"""
This is intended to be a simulation of an arbitrary network
interface or HBA, along with the s/w costs of using it (which
could include the costs of the protocol stack) above it.
"""

import SimCPU
from units import MEG, GIG, SECOND


class IFC:
    """ Performance Modeling NIC or HBA Simulation """

    def __init__(self, name, bw=1 * GIG, processor=None):
        """ create an interface simulation
            name -- name of the simulated device
            bw -- max read/write (bytes/sec)
            processor -- processor we're connected to
        """

        self.desc = name
        self.max_read_bw = bw       # maximum read bandwidth (B/s)
        self.max_write_bw = bw      # maximum write bandwidth (B/s)

        # if we don't have a processor, make one up
        if processor is None:
            self.cpu = SimCPU.CPU("generic")
        else:
            self.cpu = processor

        # these will be all provided in the derived sub-classes
        self.cpu_per_read = 0       # CPU cost (us) for the null read
        self.cpu_per_write = 0      # CPU cost (us) for the null write
        self.min_read_latency = 0   # minimum latency (us) for any read
        self.min_write_latency = 0  # minimum latency (us) for any write
        self.cpu_read_x = 0         # per byte multipler on processing time
        self.cpu_write_x = 0        # per byte multipler on processing time
        self.mem_read_x = 0         # per byte multiplier on memory refs
        self.mem_write_x = 0        # per byte multiplier on memory refs

    def read_time(self, bytes):
        """ return the elapsed time for the specified transfer """
        return self.min_read_latency + (SECOND * bytes / self.max_read_bw)

    def write_time(self, bytes):
        """ return the elapsed time for the specified transfer """
        return self.min_write_latency + (SECOND * bytes / self.max_write_bw)

    def read_cpu(self, bytes):
        """ return the CPU cost for the specified transfer """
        cpu = self.cpu.dma_us() + self.cpu.thread_us()      # DMA start/finish
        cpu += self.cpu_per_read                            # process any read
        cpu += self.mem_read_x * self.cpu.mem_read(bytes)   # memory hits
        cpu += self.cpu_read_x * self.cpu.process(bytes)    # process the data
        return cpu

    def write_cpu(self, bytes):
        """ return the CPU cost for the specified transfer """
        cpu = self.cpu.dma_us() + self.cpu.thread_us()      # DMA start/finish
        cpu += self.cpu_per_write                           # process any write
        cpu += self.mem_write_x * self.cpu.mem_write(bytes)  # memory hits
        cpu += self.cpu_write_x * self.cpu.process(bytes)    # process the data
        return cpu

    def queue_length(self, rho, max_depth=1000):
        """ average queue depth as a function of load
            rho -- average fraction of time NIC is busy
            max_depth -- the longest the queue can possibly be
        """
        if (rho >= 1):
            return max_depth
        else:
            avg = rho / (1 - rho)
            return avg if avg < max_depth else max_depth


class NIC(IFC):

    def __init__(self, name="NIC", bw=10 * GIG, processor=None):
        """ create an Network Interface Card simulation
            name -- name of the simulated device
            bw -- max read/write (bits/sec)
            processor -- processor we're connected to
        """

        n = "%dGb %s" % (bw / GIG, name)
        IFC.__init__(self, n, bw / 8, processor)

        # software TCP/IP is pretty expensive
        # FIX ... all of these TCP/NIC costs are made up
        self.cpu_read_x = 5         # per byte multipler on processing
        self.mem_read_x = 2         # per byte multipler on memory
        self.cpu_min_read = 3       # minimum CPU time (us) for null read
        self.min_read_latency = 5   # minimum time (us) for the null read

        self.cpu_write_x = 5        # per byte multipler on processing
        self.mem_write_x = 2        # per byte multipler on memory
        self.cpu_min_write = 3      # minimum CPU time (us) for null write
        self.min_write_latency = 5  # minimum time (us) for the null write


class HBA(IFC):
    def __init__(self, name="HBA", bw=16 * GIG, processor=None):
        """ create an HBA simulation
            name -- name of the simulated device
            bw -- max read/write (bits/sec)
            processor -- processor we're connected to
        """

        n = "%dGb %s" % (bw / GIG, name)
        IFC.__init__(self, n, bw / 8, processor=processor)

        # disk writes can be pretty efficient
        # FIX ... all of these HBA costs are made up
        self.min_read_latency = 1   # minimum time (us) for the null read
        self.min_write_latency = 1  # minimum time (us) for the null write


def makeHBA(dict, cpu):
    defaults = {
        'bw': 16 * GIG,
    }
    bw = dict['bw'] if 'bw' in dict else defaults['bw']
    hba = HBA(bw=bw, processor=cpu)
    return hba


def makeNIC(dict, cpu):
    defaults = {
        'bw': 10 * GIG,
    }
    bw = dict['bw'] if 'bw' in dict else defaults['bw']
    nic = NIC(bw=bw, processor=cpu)
    return nic


from Report import Report


def testHBA(hba, dict, descr):
    defaults = {
        'bsizes': [512, 4096, 128 * 1024, 4096 * 1024]
    }

    print(descr)
    r = Report(("cpu read", "tot read", "cpu write", "tot write"))
    r.printHeading()

    bsizes = dict['bsizes'] if 'bsizes' in dict else defaults['bsizes']
    for bs in bsizes:
        tr = hba.read_time(bs)
        cr = hba.read_cpu(bs)
        tw = hba.write_time(bs)
        cw = hba.write_cpu(bs)
        r.printLatency(bs, (cr, tr, cw, tw))
    print("")


def testNIC(nic, dict, descr):
    defaults = {
        'bsizes': [64, 128, 256, 512, 1024, 2048, 4096, 8192, 16 * 1024]
    }

    print(descr)
    r = Report(("cpu read", "tot read", "cpu write", "tot write"))
    r.printHeading()

    bsizes = dict['bsizes'] if 'bsizes' in dict else defaults['bsizes']
    for bs in bsizes:
        tr = nic.read_time(bs)
        cr = nic.read_cpu(bs)
        tw = nic.write_time(bs)
        cw = nic.write_cpu(bs)
        r.printLatency(bs, (cr, tr, cw, tw))
    print("")

#
# basic unit test exerciser
#
if __name__ == '__main__':

    hba = makeHBA({}, None)
    msg = "%s on %s" % (hba.desc, hba.cpu.desc)
    testHBA(hba, {}, msg)

    nic = makeNIC({}, None)
    msg = "%s on %s" % (nic.desc, nic.cpu.desc)
    testNIC(nic, {}, msg)
