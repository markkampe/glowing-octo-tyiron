#
# nonesuch
#

"""
This is intended to be able to simulate the overhead of
a block server that stores the data in local memory and
periodically flushes the accumulated writes to disk.
"""


import Poisson
from  units import *


def xfer(bytes, rate):
    """ time to transfer a specified number of bytes """
    return SECOND * bytes / rate


class Server:
    """ Performance Modeling Single Server Simulation. """

    warnings = ""           # I didn't want these to come out in mid test

    def __init__(self,
                 data_fs,
                 num_disks=1,
                 nic_bw=10 * GIG,
                 writeback=32 * MB,
                 objsize=4 * MB):
        """ create a file store simulation
            data_fs -- SimFS for the data file system
            num_disks -- number of data file systems per server
            nic_bw -- available network bandwidth
            writeback -- size of writeback buffer
            objsize -- maximum size of an on-disk object
        """
        self.data_fs = data_fs
        self.write_buf = writeback
        self.num_disks = num_disks
        self.nic_bw = nic_bw / 8
        self.obj_size = objsize

        # bad performance estimates ... FIX calculate or make parameters
        self.min_nic_write = 1  # us to send a minimum response
        self.min_nic_read = 1   # us to receive/dispatch a server thread
        self.dispatch = 20      # us to wake and dispatch a thread
        self.copy_bw = 768 * MB  # speed for data writes to NVRAM

    def read(self, bsize, depth=1, seq=False):
        """ expected read performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (subsequent reads from cache)
        """

        # basic times for message receipt, dispatch and response
        t_net = self.min_nic_read + xfer(bsize, self.nic_bw) \
            + self.min_nic_write
        t_dsp = self.dispatch
        bw_n = self.nic_bw

        # expected parallelism (requsts to multiple FS/disks)
        parallel = depth if depth <= self.num_disks else self.num_disks
        perdisk = 1 if parallel <= depth else depth / parallel

        # FIX ... come up with some basis for this number
        t_l_cpu = 10        # us(cpu) to find/open desired object
        # FIX ... there may be disk I/O associated with lookup before open
        # FIX ... how often will this be cached
        t_l_dsk = self.data_fs.open()

        # the actual file I/O
        if seq and bsize <= obj_size /2:
            # FIX ... should we get whole-object read benefits here
            t_fr = self.data_fs.read(bsize, self.data_fs.size,
                                     seq=False, depth=perdisk)
        elif (bsize > self.obj_size):
            # we have to break this read up
            t_fr = self.data_fs.read(self.obj_size, self.data_fs.size,
                                     seq=False, depth=perdisk)
            t_fr *= bsize / self.obj_size
        else:
            t_fr = self.data_fs.read(bsize, self.data_fs.size,
                                     seq=False, depth=perdisk)
        t_dsk = t_l_dsk + t_fr
        bw_fs = SECOND * bsize * self.num_disks / t_dsk

        # FIX ... could CPU cycles be the binding constraint
        bw_cpu = self.copy_bw

        # compute the request latency and throughputs
        latency = t_net + t_dsp + t_l_cpu + t_l_dsk + t_fr
        iops = SECOND / latency
        bw_iops = bsize * iops
        bandwidth = min(bw_iops, bw_n, bw_fs, bw_cpu)
        return (latency, bandwidth)

    def write(self, bsize, depth=1, seq=False):
        """ expected write performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (aggregate whole object writes)
        """

        # basic times for message receipt, dispatch and response
        t_net = self.min_nic_read + xfer(bsize, self.nic_bw) + \
            self.min_nic_write
        t_dsp = self.dispatch
        bw_n = self.nic_bw

        # making a local copy
        t_copy = xfer(bsize, self.copy_bw)

        # figure out how much parallelism we can get out of this load
        parallel = 1 if bsize >= self.write_buf else self.write_buf / bsize
        perdisk = 1 if (parallel <= self.num_disks) \
            else parallel / self.num_disks

        # FIX ... come up with some basis for this number
        t_l_cpu = 10        # us(cpu) to find/open desired object
        # FIX ... there may be disk I/O associated with lookup before create
        # FIX ... how often will this be an open rather than a create
        # FIX ... do the creates get deferred until the writes flush
        t_l_dsk = self.data_fs.create(sync=False)

        # figure out how long it will take to do the real writes
        if seq and bsize <= self.obj_size / 2:
            # we get to aggregate multiple writes to a single object
            t_fw = self.data_fs.write(obj_size, self.data_fs.size,
                                      seq=False, sync=False, depth=perdisk)
            t_fw /= (self.obj_size / bsize)
        elif bsize > self.obj_size:
            # we have to break this up into multiple writes
            t_fw = self.data_fs.write(self.obj_size, self.data_fs.size,
                                      seq=False, sync=False, depth=perdisk)
            t_fw *= (bsize / obj_size)
        else:
            t_fw = self.data_fs.write(bsize, self.data_fs.size,
                                      seq=False, sync=False, depth=perdisk)
        t_disk = t_l_dsk + t_fw
        bw_fs = SECOND * bsize * self.num_disks / t_disk

        # compute the overall CPU load
        t_sync = t_dsp + t_copy
        t_async = t_l_cpu
        bw_cpu = SECOND * bsize / (t_sync + t_async)

        # compute the request latency and throughputs
        latency = t_net + t_sync
        iops = SECOND / latency
        bw_iops = bsize * iops
        bandwidth = min(bw_iops, bw_n, bw_fs, bw_cpu)
        return (latency, bandwidth)

    def create(self):
        """ creation one new data containing object on the data file system """

        tmd = 10    # FIX metadata update implications of creating a new object
                    # FIX parallelism applies here too
        return self.data_fs.create() + tmd

    def delete(self):
        """ delete a data containing object from the data file system """

        tmd = 10    # FIX metadata update implications of deleting an object
                    # FIX parallelism applies here too
        return self.data_fs.delete() + tmd
