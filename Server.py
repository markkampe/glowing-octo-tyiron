#
# nonesuch
#

"""
This is intended to be able to simulate the overhead of
a block server that stores the data in local memory and
periodically flushes the accumulated writes to disk.
"""

#
# ISSUES:
#   I am neurotic about what random and sequential mean in this module
#   because we have not well-defined the loads we are simulating.  The
#   current assumptions are:
#     1. sequential means consecutive blocks to a single object file
#        and depth means many of those requests queued at the same time.
#     2. random means blocks from randomly chosen objects, and depth means
#        concurrent requests for multiple objects.
#     3. even though the standard client is likely togenerate only
#        full-stripe reads and writes, this Server simulation will
#        attempt to do what ever I/O is requested ... making it possible
#

from units import *

# constants to control queue length warnings
WARN_LOAD = 0.8             # warn if load goes above this level
WARN_DELAY = 100            # (us) only warn if queue delay goes above
WARN_DELTA = 10             # (%) only warn if queue delay increases op time by


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
            num_disks -- number of data file systems per server (for our use)
            num_nic -- number of NICs per server (for our use)
            num_cpus -- number of processors per server (for our use)
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

        # sizing performance parameters
        self.min_msg = 128                  # minimum request/response
        self.data_width = 128 * 1024        # basic unit of data transfer
        self.max_obj_size = 4 * 1024 * 1024  # maximum object length

        # magic performance tuning constants
        self.r_cpu_x = 1.0  # scaling factor for read CPU work
        self.w_cpu_x = 1.0  # scaling factor for write CPU work
        self.r_mem_x = 0.5  # scaling factor for read memory fetches
        self.w_mem_x = 1.0  # scaling factor for write memory fetches
        self.commit_us = 1  # time (us) to handle a commit FIX bogus

    def warn(self, msg):
        """ add a warning to our accumulated warnings list """

        # only if it is not already there
        if (self.warnings.find(msg) < 0):
            self.warnings += msg

    def read(self, bsize, depth=1, seq=False):
        """ expected read performance
            bsize -- size of each request
            depth -- number of parallel requests
                    seq -> to a single object per disk
            seq -- is the I/O sequential (within a single object)
                    or random (distributed over many objects). The
                    RAID striping across relatively small objects
                    makes random within an object less important.

            NOTE: it is assumed that these requests are spread across
                  all of the available disks

            NOTE: we try to simulate what the server would do, even
                  for requests that its clients do not currently generate.
                  This is to enable us to explore alternative implementations
                  and simulate results for a wider range of benchmarks.
        """

        descr = "%dK, d=%d %s reads" % \
            (bsize / 1024, depth, "seqential" if seq else "random")
        load = {}

        # network times for request receipt and response transmission
        t_net_r = self.nic.min_read_latency + self.nic.read_time(self.min_msg)
        t_net_w = self.nic.min_write_latency + \
            self.nic.write_time(self.min_msg + bsize)
        # network bandwith will be limited by the read responses
        bw_n = self.num_nics * bsize * SECOND / t_net_w

        # CPU time to process the received packet and response
        cpu_msg = self.nic.read_cpu(self.min_msg)
        cpu_msg += self.r_cpu_x * self.cpu.process(bsize)
        cpu_msg += self.r_mem_x * self.cpu.mem_read(bsize)
        cpu_msg += self.nic.write_cpu(self.min_msg + bsize)

        # figure out the cost of finding the object we read from
        (t_open, bw, l) = self.data_fs.open()
        cpu_open = l['cpu'] * SECOND    # already baked into t_open
        if bsize < self.max_obj_size:
            blk_per_read = self.max_obj_size / bsize
            if seq:
                t_open /= blk_per_read
                cpu_open /= blk_per_read
            else:
                obj_reuse = 1   # FIX estimate random I/O object reuse
                t_open /= obj_reuse

        # figure out what I/O we are actually going to do
        w = self.data_width     # the fundamental unit of file I/O
        sz = self.data_fs.size  # FIX ... is this right?
        s = seq
        if seq:
            if w >= depth * bsize:
                # minimum read is a full strip
                d = max(1, depth * bsize / w)
                req_per_read = w / bsize
            else:
                # split this request over multiple reads
                d = depth * bsize / w
                req_per_read = float(w) / bsize
            (t_fr, bw, l) = self.data_fs.read(w, sz, seq=s, depth=d)
        else:
            # figure out how many requests are for each disk
            disks = min(depth, self.num_disks)
            d = max(1, depth / disks)
            if bsize <= w:
                req_per_read = 1
            else:   # a random multi-strip read gets split up
                s = True
                req_per_read = float(w) / bsize
                d *= bsize / w

        # figure out how long it will take to do that I/O
        (t_fr, bw, l) = self.data_fs.read(w, sz, seq=s, depth=d)
        t_fr /= req_per_read
        cpu_fs = l['cpu'] * SECOND / req_per_read
        t_dsk = t_open + t_fr
        bw_fs = SECOND * bsize * self.num_disks / t_dsk

        # now that we have all the CPU costs, add up the utilization
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        tot_cpu = cpu_msg + cpu_fs + cpu_open
        bw_cpu = avail_cores * bsize * SECOND / tot_cpu

        # the HBA could become a throughput bottleneck
        bw_hba = self.num_hbas * self.hba.max_read_bw

        # compute the request latency and throughputs
        #   (we don't count t_net_r because the client pays for that)
        latency = cpu_msg + t_open + t_fr + t_net_w
        bw_base = depth * bsize * SECOND / latency
        bandwidth = min(bw_base, bw_n, bw_fs, bw_cpu, bw_hba)
        iops = bandwidth / bsize
        q_delay = 0
        load['fs'] = bandwidth / bw_fs
        load['hba'] = bandwidth / bw_hba

        # did we run out of HBA
        if (bw_hba < bw_base):
            self.warn("Server HBA caps throughput at %dMB/s for %s\n" %
                      (bw_hba / MEG, descr))

        # see what this means for NIC load and queue
        nic_load = t_net_w * iops / float(self.num_nics * SECOND)
        if (bw_n < bw_base):
            self.warn("Server NIC saturated by %dus x %d IOPS for %s\n" %
                      (t_net_w, iops, descr))
        delay = t_net_w * self.nic.queue_length(nic_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Server NIC load (%.2f) adds %dus (%d%%) to %s\n" %
                      (nic_load, delay, delta, descr))
        load['net'] = nic_load

        # see what this means for CPU load and queue
        core_load = tot_cpu * iops / float(avail_cores * SECOND)
        if (bw_cpu < bw_base):
            self.warn("Server CPUs saturated by %dus x %d IOPS for %s\n" %
                      (tot_cpu, iops, descr))
        delay = tot_cpu * self.cpu.queue_length(core_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Server CPU load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (core_load, delay, delta, descr))
        load['cpu'] = core_load

        return (latency + q_delay, bandwidth, load)

    def write(self, bsize, depth=1, seq=False):
        """ expected write performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is the I/O sequential (within a single object)
                    or random (distributed over many objects). The
                    RAID striping across relatively small objects
                    makes random within an object less important.

            NOTE: we try to simulate what the server would do, even
                  for requests that its clients do not currently generate.
                  This is to enable us to explore alternative implementations
                  and simulate results for a wider range of benchmarks.
        """

        load = {}

        descr = "%dK, d=%d %s writes" % \
            (bsize / 1024, depth, "seqential" if seq else "random")

        # basic wire times for message receipt, dispatch and response
        t_net_r = self.nic.min_read_latency + \
            self.nic.read_time(self.min_msg + bsize)
        t_net_w = self.nic.min_write_latency + \
            self.nic.write_time(self.min_msg)
        # network bandwith will be limited by the incoming requests
        bw_n = self.num_nics * bsize * SECOND / t_net_r

        # CPU time to process the received packet, copy it, and send response
        t_dsp = self.nic.read_cpu(self.min_msg + bsize)
        t_cpu = self.w_cpu_x * self.cpu.process(bsize)
        t_cpu += self.w_mem_x * self.cpu.mem_write(bsize)
        t_rsp = self.nic.write_cpu(self.min_msg)

        # figure out the cost of open/creating the object we write to
        # HELP ... work out the stat/open/create scenarios
        (t_crt, bw, l) = self.data_fs.create(sync=False)
        cpu_crt = l['cpu'] * SECOND     # this is already baked into t_crt
        if bsize < self.max_obj_size:
            if seq:
                blk_per_write = self.max_obj_size / bsize
                t_crt /= blk_per_write
                cpu_crt /= blk_per_write
            else:
                obj_reuse = 1   # FIX estimate random I/O object reuse
                t_crt /= obj_reuse

        # figure out how long it will take to flush the NVRAM to disk
        w = self.data_width
        d = self.write_buf / (w * self.num_disks)
        sz = self.data_fs.size  # FIX ... is this right?
        (t_fw, bw, l) = self.data_fs.write(w, sz, seq=seq, sync=False, depth=d)
        t_fw = (t_fw * bsize) / w
        t_disk = t_crt + t_fw
        t_async = l['cpu'] * SECOND
        bw_fs = SECOND * bsize * self.num_disks / t_disk

        # the HBA could become a throughput bottleneck
        bw_hba = self.num_hbas * self.hba.max_read_bw

        # compute the overall CPU load
        t_sync = t_dsp + t_rsp + t_cpu
        cpu_per_op = t_sync + t_async + cpu_crt
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        bw_cpu = avail_cores * SECOND * bsize / cpu_per_op

        # compute the request latency and throughputs
        #   (we don't count t_net_r because the caller pays for that)
        latency = t_net_w + t_sync
        bw_base = depth * bsize * SECOND / latency
        bandwidth = min(bw_base, bw_n, bw_fs, bw_cpu, bw_hba)
        iops = bandwidth / bsize
        q_delay = 0

        load['fs'] = bandwidth / bw_fs
        load['hba'] = bandwidth / bw_hba

        # did we run out of HBA
        if (bw_hba < bw_base):
            self.warn("Server HBA caps throughput at %dMB/s for %s\n" %
                      (bw_hba / MEG, descr))

        # see what this means for NIC load and queue
        if (bw_n < bw_base):
            self.warn("Server NIC saturated by %dus x %d IOPS for %s\n" %
                      (t_net_r, iops, descr))
        nic_load = t_net_w * iops / float(self.num_nics * SECOND)
        delay = t_net_w * self.nic.queue_length(nic_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Server NIC load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (nic_load, delay, delta, descr))
        load['net'] = nic_load

        # see what this means for CPU load and queue
        if (bw_cpu < bw_base):
            self.warn("Server CPUs saturated by %dus x %d IOPS for %s\n" %
                      (cpu_per_op, iops, descr))
        core_load = cpu_per_op * iops / float(avail_cores * SECOND)
        delay = cpu_per_op * self.cpu.queue_length(core_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Server CPU load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (core_load, delay, delta, descr))
        load['cpu'] = core_load

        return (latency + q_delay, bandwidth, load)

    def commit(self):
        """ expected commit performance
        """

        # basic wire times for message receipt, dispatch and response
        t_net_r = self.nic.min_read_latency + \
            self.nic.read_time(self.min_msg)
        t_net_w = self.nic.min_write_latency + \
            self.nic.write_time(self.min_msg)
        bw_n = self.num_nics * SECOND / t_net_w

        # CPU time to process the received packet, copy it, and send response
        t_dsp = self.nic.read_cpu(self.min_msg)
        t_cpu = self.commit_us
        t_rsp = self.nic.write_cpu(self.min_msg)

        # and assemble the results for reporting
        load = {}
        cpu_per_op = t_dsp + t_cpu + t_rsp
        latency = cpu_per_op + t_net_w
        iops = SECOND / latency
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        core_load = cpu_per_op * iops / float(avail_cores * SECOND)
        load['cpu'] = core_load
        nic_load = t_net_w * iops / float(self.num_nics * SECOND)
        load['net'] = nic_load

        return(latency, bw, load)

    def getattr(self, cached=0, depth=1):
        """ expected time for getattrs
            cached -- fraction of requests for objects in cache
            depth -- number of concurrent parallel requests
        """

        # basic wire times for message receipt, dispatch and response
        t_net_r = self.nic.min_read_latency + \
            self.nic.read_time(self.min_msg)
        t_net_w = self.nic.min_write_latency + \
            self.nic.write_time(self.min_msg)
        bw_n = self.num_nics * SECOND / t_net_w

        # CPU time to process the received packet, and send response
        t_dsp = self.nic.read_cpu(self.min_msg)
        t_rsp = self.nic.write_cpu(self.min_msg)

        # FIX - bogus modeling of getattr implementation
        (t_fsg, bw_fsg, l_fsg) = self.data_fs.getattr()
        t_fsg *= (1 - cached)
        cpu_fsg = l_fsg['cpu'] * SECOND * (1 - cached)

        # and assemble the results for reporting
        load = {}
        cpu_per_op = t_dsp + cpu_fsg + t_rsp
        latency = t_dsp + t_fsg + t_rsp + t_net_w
        iops = SECOND / latency
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        core_load = cpu_per_op * iops / float(avail_cores * SECOND)
        load['cpu'] = core_load
        nic_load = t_net_w * iops / float(self.num_nics * SECOND)
        load['net'] = nic_load
        # load['fs'] = l_fsg['fs']

        return(latency, iops, load)

    def setattr(self, cached=0, depth=1, sync=False):
        """ expected time for getattrs
            cached -- fraction of requests for objects in cache
            depth -- number of concurrent parallel requests
            sync -- must these changes be persisted immediately
        """

        # basic wire times for message receipt, dispatch and response
        t_net_r = self.nic.min_read_latency + \
            self.nic.read_time(self.min_msg)
        t_net_w = self.nic.min_write_latency + \
            self.nic.write_time(self.min_msg)
        bw_n = self.num_nics * SECOND / t_net_w

        # CPU time to process the received packet, and end response
        t_dsp = self.nic.read_cpu(self.min_msg)
        t_rsp = self.nic.write_cpu(self.min_msg)

        # FIX - bogus modeling of setattr implementation
        (t_fsg, bw_fsg, l_fsg) = self.data_fs.getattr()
        t_fsg *= (1 - cached)
        cpu_fsg = l_fsg['cpu'] * SECOND * (1 - cached)
        # l_fsg['fs'] *= (1 - cashed)
        (t_fss, bw_fss, l_fss) = self.data_fs.setattr(sync=sync)
        cpu_fss = l_fss['cpu'] * SECOND

        # and assemble the results for reporting
        load = {}
        cpu_per_op = t_dsp + cpu_fsg + cpu_fss + t_rsp
        latency = t_dsp + t_fsg + t_fss + t_rsp + t_net_w
        iops = SECOND / latency
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        core_load = cpu_per_op * iops / float(avail_cores * SECOND)
        nic_load = t_net_w * iops / float(self.num_nics * SECOND)
        load['cpu'] = core_load
        load['net'] = nic_load
        # load['fs'] = l_fsg['fs'] + l_fss['fs']

        return(latency, iops, load)
