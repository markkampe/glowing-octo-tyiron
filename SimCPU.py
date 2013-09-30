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
This is intended to be a simulation of processor speed and throughput
"""

from units import MEG, GIG, SECOND


class CPU:
    """ Performance Modeling NIC or HBA Simulation """

    def __init__(self, name, cores=1, speed=3 * GIG, ddr=1600 * MEG):
        """ create an interface simulation
            name -- name of the simulated processor
            cores -- number of cores per chip
            speed -- clock speed in hz
        """

        self.desc = "%4.1fGhz %s" % (speed / GIG, name)
        self.cores = cores              # cores per chip
        self.mhz = speed / MEG          # clock speed
        self.hyperthread = 1.3          # hyperthreading multiplier
        width = 8                       # bus width (bytes)

        # wild guess cost constants
        intr = 30000        # FIX madeup   interrupt svc (cycles)
        disp = 100000       # FIX madeup   thread dispatch (cycles)
        dma = 30000         # FIX madeup   DMA setup (cycles)

        # estimated capabilities
        self.bus_bw = speed * width     # max bus transfer rate (B/s)
        self.mem_bw = ddr * width       # max mem xfr (B/s)
        self.intr_us = intr / self.mhz  # interrupt service (us)
        self.disp_us = disp / self.mhz  # thread dispatch (us)
        self.dma_us = dma / self.mhz    # DMA setup (us)

    def mem_read(self, bytes):
        """ return the elapsed time to read that number of uncached bytes """
        bw = min(self.bus_bw, self.mem_bw)
        return bytes * SECOND / bw

    def mem_write(self, bytes):
        """ return the elapsed time to write that number of bytes """
        bw = min(self.bus_bw, self.mem_bw)
        return bytes * SECOND / bw

    def process(self, bytes):
        """ return the elapsed time to process that number of bytes """
        return bytes * SECOND / self.bus_bw
