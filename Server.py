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

WARN_LEVEL = 0.8            # max expected load on pass-through resources


class Server:
    """ Performance Modeling Single Server Simulation. """

    warnings = ""           # I didn't want these to come out in mid test

    def __init__(self,
                 data_fs,
                 nic,
                 hba,
                 cpu,
                 num_disks=1,
                 num_nics=1,
                 num_hbas=1,
                 num_cpus=1,
                 writeback=32 * MB):
        """ create an object server simulation
            data_fs -- SimFS for the data file system
            nic -- SimIFC for the network interface
            hba -- SIMIFC for the HBA
            cpu -- SimCPU for the processor
            num_disks -- number of data file systems per server
            num_nic -- number of NICs per server
            num_cpus -- number of processors per server
            writeback -- size of writeback buffer
        """
        self.data_fs = data_fs
        self.nic = nic
        self.hba = hba
        self.cpu = cpu
        self.num_disks = num_disks
        self.num_nics = num_nics
        self.num_hbas = num_hbas
        self.num_cpus = num_cpus
        self.write_buf = writeback
        self.obj_size = 4 * MB
        self.min_msg = 128

    def read(self, bsize, depth=1, seq=False):
        """ expected read performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (subsequent reads from cache)
        """

        descr = "%dK, d=%d %s reads" % \
            (bsize / 1024, depth, "seqential" if seq else "random")

        # we start out with no queuing delays
        t_queue = 0

        # network times for request receipt and response transmission
        t_net = self.nic.min_read_latency + self.nic.read_time(self.min_msg)
        t_net += self.nic.min_write_latency + \
            self.nic.write_time(self.min_msg + bsize)
        bw_n = self.num_nics * self.nic.max_read_bw
        delay = 0  # FIX - compute NIC queueing delays
        if (delay > 0):
            self.warnings += "Server NIC load adds %dus to %s\n" % \
                (delay, descr)
            t_queue += delay

        # CPU time to process the received packet and response
        t_dsp = self.nic.read_cpu(self.min_msg)
        t_rsp = self.nic.write_cpu(self.min_msg + bsize)
        t_cpu = self.cpu.process(bsize)
        bw_cpu = bsize * SECOND / (t_dsp + t_rsp + t_cpu)
        bw_cpu *= self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        delay = 0  # FIX - compute CPU queueing delays
        if (delay > 0):
            self.warnings += "Server CPU load adds %dus to %s\n" % \
                (delay, descr)
            t_queue += delay

        # expected parallelism (requsts to multiple FS/disks)
        parallel = depth if depth <= self.num_disks else self.num_disks
        perdisk = 1 if parallel <= depth else depth / parallel

        # FIX ... come up with some basis for this number
        t_l_cpu = 10        # us(cpu) to find/open desired object
        # FIX ... there may be disk I/O associated with lookup before open
        # FIX ... how often will this be cached
        t_l_dsk = self.data_fs.open()

        # the actual file I/O
        if seq and bsize <= self.obj_size / 2:
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
        bw_hba = self.num_hbas * self.hba.max_read_bw
        delay = 0  # FIX - compute HBA queueing delays
        if (delay > 0):
            self.warnings += "Server HBA load adds %dus to %s\n" % \
                (delay, descr)
            t_queue += delay

        # compute the request latency and throughputs
        latency = t_net + t_dsp + t_l_cpu + t_l_dsk + t_fr + t_cpu + t_rsp
        latency += t_queue
        iops = depth * SECOND / latency
        bandwidth = min(bsize * iops, bw_n, bw_fs, bw_cpu, bw_hba)
        load = {
            'net': bandwidth / bw_n,
            'cpu': bandwidth / bw_cpu,
            'fs': bandwidth / bw_fs,
            'hba': bandwidth / bw_hba
        }

        # check for unexpected throughput caps
        for key in ('net', 'cpu', 'hba'):
            l = load[key]
            if l > 0.99:
                self.warnings += "Server throughput is capped by %s for %s\n" \
                    % (key, descr)
            elif l >= WARN_LEVEL:
                self.warnings += "Server %s load at %4.2f for %s\n" % \
                    (key, l, descr)

        return (latency, bandwidth, load)

    def write(self, bsize, depth=1, seq=False):
        """ expected write performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (aggregate whole object writes)
        """

        descr = "%dK, d=%d %s writes" % \
            (bsize / 1024, depth, "seqential" if seq else "random")

        # we start out with no queuing delays
        t_queue = 0

        # basic times for message receipt, dispatch and response
        t_net = self.nic.min_read_latency + \
            self.nic.read_time(self.min_msg + bsize)
        t_net += self.nic.min_write_latency + self.nic.write_time(self.min_msg)
        bw_n = self.num_nics * self.nic.max_read_bw
        delay = 0  # FIX - compute NIC queueing delays
        if (delay > 0):
            self.warnings += "Server NIC load adds %dus to %s\n" % \
                (delay, descr)
            t_queue += delay

        # CPU time to process the received packet, copy it, and send response
        t_dsp = self.nic.read_cpu(self.min_msg + bsize)
        t_rsp = self.nic.write_cpu(self.min_msg)
        t_cpu = self.cpu.mem_read(bsize) + self.cpu.mem_write(bsize)
        delay = 0  # FIX - compute CPU queueing delays
        if (delay > 0):
            self.warnings += "Server CPU load adds %dus to %s\n" % \
                (delay, descr)
            t_queue += delay

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
            t_fw = self.data_fs.write(self.obj_size, self.data_fs.size,
                                      seq=False, sync=False, depth=perdisk)
            t_fw /= (self.obj_size / bsize)
        elif bsize > self.obj_size:
            # we have to break this up into multiple writes
            t_fw = self.data_fs.write(self.obj_size, self.data_fs.size,
                                      seq=False, sync=False, depth=perdisk)
            t_fw *= (bsize / self.obj_size)
        else:
            t_fw = self.data_fs.write(bsize, self.data_fs.size,
                                      seq=False, sync=False, depth=perdisk)
        t_disk = t_l_dsk + t_fw
        bw_fs = SECOND * bsize * self.num_disks / t_disk
        bw_hba = self.num_hbas * self.hba.max_write_bw
        delay = 0  # FIX - compute HBA queueing delays
        if (delay > 0):
            self.warnings += "Server HBA load adds %dus to %s\n" % \
                (delay, descr)
            t_queue += delay
        # FIX ... do I have to add some CPU costs for the write as well

        # compute the overall CPU load
        t_sync = t_dsp + t_rsp + t_cpu
        t_async = t_l_cpu
        bw_cpu = SECOND * bsize / (t_sync + t_async)
        bw_cpu *= self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        # FIX - add CPU queuing delays

        # compute the request latency and throughputs
        latency = t_net + t_sync + t_queue
        iops = depth * SECOND / latency
        bandwidth = min(bsize * iops, bw_n, bw_fs, bw_cpu, bw_hba)
        load = {
            'net': bandwidth / bw_n,
            'cpu': bandwidth / bw_cpu,
            'fs': bandwidth / bw_fs,
            'hba': bandwidth / bw_hba
        }

        # check for unexpected throughput caps
        for key in ('net', 'cpu', 'hba'):
            l = load[key]
            if l > 0.99:
                self.warnings += "Server throughput is capped by %s for %s\n" \
                    % (key, descr)
            elif l >= WARN_LEVEL:
                self.warnings += "Server %s load at %4.2f for %s\n" % \
                    (key, l, descr)

        return (latency, bandwidth, load)

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
