#!/usr/bin/python
#
# nonesuch
#

"""
This is intended to be able to simulate the performance of
a distributed lock manager
"""

from units import *


class DLM:
    """ Performance Modeling Single Server Simulation. """

    warnings = ""           # I didn't want these to come out in mid test

    def __init__(self,
                 nic,
                 cpu,
                 num_nics=1,
                 num_cpus=1):

        """ create an object server simulation
            nic -- SimIFC for the network interface
            cpu -- SimCPU for the processor
            num_nic -- number of NICs per server (for our use)
            num_cpus -- number of processors per server (for our use)
        """
        self.nic = nic
        self.cpu = cpu
        self.num_nics = num_nics
        self.num_cpus = num_cpus

        # sizing performance parameters
        self.min_msg = 128	    # minimum request/response

        # magic performance tuning constants
        self.lock_us = 1        # time (us) to handle a lock

    def warn(self, msg):
        """ add a warning to our accumulated warnings list """

        # only if it is not already there
        if (self.warnings.find(msg) < 0):
            self.warnings += msg

    def lock(self):
        """ expected performance for an uncontested lock
        """

        load = {}

        # network times for request receipt and response transmission
        t_net_r = self.nic.min_read_latency + self.nic.read_time(self.min_msg)
        t_net_w = self.nic.min_write_latency + \
            self.nic.write_time(self.min_msg)
        bw_n = self.num_nics * SECOND / t_net_w     # responses limit the bw

        # CPU time to process the received packet and response
        cpu_msg = self.nic.read_cpu(self.min_msg)
        cpu_lock = self.lock_us
        cpu_msg += self.nic.write_cpu(self.min_msg)

        # FIX - we do not model persisting of lock status

        latency = cpu_msg + cpu_lock + t_net_w
        bw = SECOND / latency
        load['cpu'] = float(cpu_msg + cpu_lock) / SECOND
        return (latency, bw, load)


def makeDLM(dict):
    """ instantiate the DLM described by a configuration dict
        dict -- of DLM parameters
    """

    dflts = {
        'cpus': 1,
        'cpu': 'generic',
        'speed': 2.7 * GIG,
        'cores': 1,
        'nics': 1,
        'nic':  10 * GIG,
    }

    # collect the parameters
    cpus = dict['cpus'] if 'cpus' in dict else dflts['cpus']
    cpu = dict['cpu'] if 'cpu' in dict else dflts['cpu']
    speed = dict['speed'] if 'speed' in dict else dflts['speed']
    cores = dict['cores'] if 'cores' in dict else dflts['cores']
    nics = dict['nics'] if 'nics' in dict else dflts['nics']
    nic_bw = dict['nic'] if 'nic' in dict else dflts['nic']

    # instantiate the parts
    import SimCPU
    myCpu = SimCPU.makeCPU(dict)
    import SimIFC
    myNic = SimIFC.NIC("eth", processor=myCpu, bw=nic_bw)

    # instantiate the DLM
    dlm = DLM(myNic, myCpu, nics, cpus)
    return dlm


#
# instantiate and test the DLM simulation
#
if __name__ == '__main__':

    dlm = makeDLM({})

    (tl, bw, ll) = dlm.lock()
    print("\tlock = %dus" % (tl))
    print("")
