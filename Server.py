#
# nonesuch
#
#   FIX this simulation does not include HBA throughput limitations

"""
This is intended to be able to simulate the overhead of
a block server that stores the data in local memory and
periodically flushes the accumulated writes to disk.
"""

from  units import *


class Server:
    """ Performance Modeling Single Server Simulation. """

    warnings = ""           # I didn't want these to come out in mid test

    def __init__(self,
                 data_fs,
                 nic,
                 cpu,
                 num_disks=1,
                 num_nics=1,
                 num_cpus=1,
                 writeback=32 * MB,
                 objsize=4 * MB):
        """ create a file store simulation
            data_fs -- SimFS for the data file system
            nic -- SimIFC for the network interface
            cpu -- SimCPU for the processor
            num_disks -- number of data file systems per server
            num_nic -- number of NICs per server
            num_cpus -- number of processors per server
            writeback -- size of writeback buffer
            objsize -- maximum size of an on-disk object
        """
        self.data_fs = data_fs
        self.nic = nic
        self.cpu = cpu
        self.num_disks = num_disks
        self.num_nics = num_nics
        self.num_cpus = num_cpus
        self.write_buf = writeback
        self.obj_size = objsize
        self.min_msg = 128

    def read(self, bsize, depth=1, seq=False):
        """ expected read performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (subsequent reads from cache)
        """

        # network times for request receipt and response transmission
        t_net = self.nic.min_read_latency + self.nic.read_time(self.min_msg)
        t_net += self.nic.min_write_latency + self.nic.write_time(bsize)
        bw_n = self.num_nics * self.nic.max_read_bw

        # CPU time to process the received packet and response
        t_dsp = self.nic.read_cpu(bsize)
        t_rsp = self.nic.write_cpu(self.min_msg)
        t_cpu = self.cpu.process(bsize)
        bw_cpu = bsize * SECOND / (t_dsp + t_rsp + t_cpu)
        bw_cpu *= self.num_cpus * self.cpu.cores * self.cpu.hyperthread

        # expected parallelism (requsts to multiple FS/disks)
        parallel = depth if depth <= self.num_disks else self.num_disks
        perdisk = 1 if parallel <= depth else depth / parallel

        # FIX ... come up with some basis for this number
        t_l_cpu = 10        # us(cpu) to find/open desired object
        # FIX ... there may be disk I/O associated with lookup before open
        # FIX ... how often will this be cached
        t_l_dsk = self.data_fs.open()

        # the actual file I/O
        if seq and bsize <= obj_size / 2:
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

        # compute the request latency and throughputs
        latency = t_net + t_dsp + t_l_cpu + t_l_dsk + t_fr + t_cpu + t_rsp
        iops = SECOND / latency
        bw_iops = bsize * iops
        bandwidth = min(bw_iops, bw_n, bw_fs, bw_cpu)
        if (bandwidth == bw_n) and "NIC caps" not in self.warnings:
            msg = "\n\tServer NIC caps throughput for %d byte reads"
            self.warnings += msg % (bsize)

        return (latency, bandwidth)

    def write(self, bsize, depth=1, seq=False):
        """ expected write performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (aggregate whole object writes)
        """

        # basic times for message receipt, dispatch and response
        t_net = self.nic.min_read_latency + self.nic.read_time(bsize)
        t_net += self.nic.min_write_latency + self.nic.write_time(self.min_msg)
        bw_n = self.num_nics * self.nic.max_read_bw

        # CPU time to process the received packet, copy it, and send response
        t_dsp = self.nic.read_cpu(self.min_msg)
        t_rsp = self.nic.write_cpu(bsize)
        t_cpu = self.cpu.mem_read(bsize) + self.cpu.mem_write(bsize)

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
        # FIX ... do I have to add some CPU costs for the write as well

        # compute the overall CPU load
        t_sync = t_dsp + t_rsp + t_cpu
        t_async = t_l_cpu
        bw_cpu = SECOND * bsize / (t_sync + t_async)
        bw_cpu *= self.num_cpus * self.cpu.cores * self.cpu.hyperthread

        # compute the request latency and throughputs
        latency = t_net + t_sync
        iops = SECOND / latency
        bw_iops = bsize * iops
        bandwidth = min(bw_iops, bw_n, bw_fs, bw_cpu)
        if (bandwidth == bw_n) and "NIC caps" not in self.warnings:
            msg = "\n\tServer NIC caps throughput for %d byte writes"
            self.warnings += msg % (bsize)
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