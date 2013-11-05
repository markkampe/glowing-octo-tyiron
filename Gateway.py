#
# nonesuch
#

"""
This is a simulation of a single protocol gateway server that
stripes (and erasure codes) its data across multiple storage
servers.
"""
# HELP: I implemented pre-fetch in the Server, should it be in Gateway:read?

from Dlm import DLM
from units import KB, SECOND

# constants to control queue length warnings
WARN_LOAD = 0.8             # warn if load goes above this level
WARN_DELAY = 100            # (us) only warn if queue delay goes above
WARN_DELTA = 10             # (%) only warn if queue delay increases op time by


class Gateway:
    """ Performance Modeling Gateway Server Simulation. """

    warnings = ""           # save these up for reporting later

    def __init__(self,
                 server,
                 dlm,
                 front_nic,
                 back_nic,
                 cpu,
                 num_servers=1,
                 num_front=1,
                 num_back=1,
                 num_cpus=1,
                 n=5,
                 m=2,
                 strip=128 * KB):

        """ create a Gateway server simulation
            server -- simulation for the file server nodes
            nic -- SimIFC for the network interface
            cpu -- SimCPU for the processor
            num_servers -- number of file servers
            num_nic -- number of NICs per server
            num_cpus -- number of processors per server
            n -- number of data blocks in a stripe
            m -- number of parity blocks in a stripe
            strip -- width of stripe we write to one server
        """
        self.server = server
        self.dlm = dlm
        self.front = front_nic
        self.back = back_nic
        self.cpu = cpu
        self.num_servers = num_servers
        self.num_fronts = num_front
        self.num_backs = num_back
        self.num_cpus = num_cpus
        self.n = n
        self.m = m
        self.width = strip
        self.read_ahead = True

        # magic constants
        self.min_msg = 128      # size of a minimal request/rsponse
        self.read_mult = 2      # multipler on read request processing
        self.read_mem_x = n     # multiplier on memory read processing
        self.write_mult = 3     # multipler on write request processing
        self.write_mem_x = n + m     # multiplier on memory write processing

    def warn(self, msg):
        """ add a warning to our accumulated warnings list """

        # only if it is not already there
        if (self.warnings.find(msg) < 0):
            self.warnings += msg

    def read(self, bsize, depth=1, seq=False):
        """ expected read performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (subsequent reads from cache)
        """

        descr = "%dK, d=%d %s reads" % \
            (bsize / 1024, depth, "seqential" if seq else "random")

        # LATER - if no back-side NIC put all traffic on the front

        # network times for request receipt and response transmission
        #
        # NOTE: all network I/O contributes CPU load, but we only
        #	count the wire-time for messages we send.  The wire
        #	time for the messages we receive are included in the
        #	operation times for the components that sent them
        #	(otherwise we would be double-counting those times)
        #
        Lfr = self.front.min_read_latency
        Lfw = self.front.min_write_latency
        Lbr = self.back.min_read_latency
        Lbw = self.back.min_write_latency
        req = self.min_msg
        rsp = self.min_msg + bsize

        # cost of receiving and processing original request
        t_front_r = Lfr + self.front.read_time(req)
        t_cpu = self.front.read_cpu(req)

        # cost of obtaining full stripe locks
        #   (assume no conflicts and no explicit releases)
        if (seq):
            P_lock = float(bsize) / (self.width * self.n)
        else:
            P_lock = 1      # LATER: even random I/O gets lock reuse
        t_back_w = P_lock * (Lbw + self.back.write_time(req))
        (t, bw, l) = self.dlm.lock()
        t_lock = P_lock * t
        t_back_r = P_lock * (Lbw + self.back.read_time(req))
        t_cpu += P_lock * (self.back.read_cpu(req))
        t_cpu += P_lock * (self.back.write_cpu(req))
        bw_dlm = bsize * SECOND / t_lock

        # figure out what I/O we will actually do
        stripe = self.width * self.n    # we do all reads in full stripes
        s = seq
        prefetch = 1
        if bsize > stripe:
            # large reads get broken up
            req_per_read = float(stripe) / bsize
            d = depth * bsize / stripe
            s = True
        elif seq:
            # we can serve multiple requests from a single read
            req_per_read = stripe / bsize
            d = max(1, depth / req_per_read)
            if self.read_ahead:
                # FIX - crude prefetch modeling
                # it is not obvious how to model pre-fetch in steady-state ...
                #    it starts out as deeper reads, but then drops to 1/1
                #    it has the potential to eliminate much response waiting
                # I'm thinking that the right modeling is:
                #   add up all of the send/server/receive/process time
                #   compare this with the average inter-request time (/depth)
                #   the wait time becomes max(0, inter_req_time - service_time)
                #   but there is no savings in CPU/network or any other cost
                d *= 2          # and queue an extra pre-fetch read too
                prefetch = 2    # say it eliminates half of the server delay
        else:
            # we are reading too much
            req_per_read = 1
            d = depth

        # compute the (amortized) costs of those read requests
        (t_svr, bw_svr, l_svr) = self.server.read(self.width, d, s)
        t_svr /= prefetch * req_per_read
        t_cpu += self.n * self.back.write_cpu(req) / req_per_read
        t_cpu += self.n * self.back.read_cpu(rsp) / req_per_read
        t_back_w += self.n * (Lbw + self.back.write_time(req)) / req_per_read
        t_back_r += self.n * (Lbr + self.back.read_time(rsp)) / req_per_read

        # scale the returned server bandwidth for the entire cluster
        #   NOTE: this is a highly theoretical number
        bw_svr *= self.num_servers

        # CPU time to process actually process the data
        t_cpu = self.read_mult * self.cpu.process(bsize)
        t_cpu += self.read_mem_x * self.cpu.mem_read(bsize)

        # cost of sending the response back to the client
        t_front_w = Lfw + self.front.write_time(rsp)          # send response
        t_cpu += self.front.write_cpu(rsp)

        # compute available network bandwidth
        #	NICs are full duplex, but we only look at the side that
        #	sees the most traffic for the simulated operation
        #   (in this case, back-side reads and front-side writes)
        bw_nf = self.num_fronts * bsize * SECOND / t_front_w
        bw_nb = self.num_backs * bsize * SECOND / t_back_r

        # compute available CPU bandwidth
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        bw_cpu = avail_cores * bsize * SECOND / t_cpu

        # compute the request latency and throughputs
        latency = t_front_w + t_back_w + t_cpu + t_lock + t_svr
        bw_base = depth * bsize * SECOND / latency
        bandwidth = min(bw_base, bw_dlm, bw_svr, bw_nf, bw_nb, bw_cpu)
        iops = bandwidth / bsize
        q_delay = 0

        load = {}
        load['server'] = bandwidth / bw_svr
        load['dlm'] = bandwidth / bw_dlm

        # see what this means for front NIC load and queue
        if (bw_nf < bw_base):
            self.warn("Gateway front saturated by %dus x %d IOPS for %s\n" %
                      (t_front_w, iops, descr))
        nic_load = t_front_w * iops / float(self.num_fronts * SECOND)
        if (nic_load >= 0.99):
            self.warn("Gateway front saturated by %dus x %d IOPS for %s\n" %
                      (t_front_w, iops, descr))
        delay = t_front_w * self.front.queue_length(nic_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Gateway front load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (nic_load, delay, delta, descr))
        load['front'] = nic_load

        # see what this means for back NIC load and queue
        if (bw_nb < bw_base):
            self.warn("Gateway back saturated by %dus x %d IOPS for %s\n" %
                      (t_back_r, iops, descr))
        nic_load = t_back_w * iops / float(self.num_backs * SECOND)
        delay = t_back_w * self.back.queue_length(nic_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Gateway back load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (nic_load, delay, delta, descr))
        load['back'] = nic_load

        # see what this means for CPU load and queue
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        core_load = t_cpu * iops / float(avail_cores * SECOND)
        if (bw_cpu < bw_base):
            self.warn("Gateway CPUs saturated by %dus x %d IOPS for %s\n" %
                      (t_cpu, iops, descr))
        delay = t_cpu * self.cpu.queue_length(core_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Gateway CPU load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (core_load, delay, delta, descr))
        load['cpu'] = core_load

        return (latency + q_delay, bandwidth, load)

    def write(self, bsize, depth=1, seq=False):
        """ expected write performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (subsequent writest to same objs)
        """

        descr = "%dK, d=%d %s writes" % \
            (bsize / 1024, depth, "seqential" if seq else "random")

        # LATER - if no back-side NIC put all traffic on the front

        # network times for request receipt and response transmission
        #
        # NOTE: all network I/O contributes CPU load, but we only
        #	count the wire-time for messages we send.  The wire
        #	time for the messages we receive are included in the
        #	operation times for the components that sent them
        #	(otherwise we would be double-counting those times)
        #
        small = self.min_msg
        large = self.min_msg + bsize
        LfR = self.front.min_read_latency + self.front.read_time(large)
        Lfw = self.front.min_read_latency + self.front.read_time(small)
        Lbr = self.front.min_read_latency + self.front.read_time(small)
        Lbw = self.front.min_read_latency + self.front.read_time(small)
        LbR = self.front.min_read_latency + self.front.read_time(large)
        LbW = self.front.min_read_latency + self.front.read_time(large)

        # cost of receiving and processing original request
        t_front_r = LfR
        t_cpu = self.front.read_cpu(large)

        # cost of obtaining full stripe locks
        #   (assume no conflicts and no explicit releases)
        if (seq):
            P_lock = float(bsize) / (self.width * self.n)
        else:
            P_lock = 1      # LATER: even random I/O gets some lock reuse
        t_back_w = P_lock * Lbw
        (t, bw, l) = self.dlm.lock()
        t_lock = P_lock * t
        t_back_r = P_lock * Lbr
        t_cpu += P_lock * (self.back.read_cpu(small))
        t_cpu += P_lock * (self.back.write_cpu(small))
        bw_dlm = bsize * SECOND / t_lock

        # CPU time to process/check-sum/etc this write
        t_cpu = self.write_mult * self.cpu.process(bsize)
        t_cpu += self.write_mem_x * self.cpu.mem_write(bsize)

        # figure out what I/O we will actually do
        #   NOTE: we only wait one server read/write time because all
        #         of the reads and writes happen in parallel
        stripe = self.width * self.n    # we do all writes in full stripes
        s = seq
        if bsize > stripe:
            # large writes get broken into stripes
            d = depth * bsize / stripe
            (t_s_w, bw_svr, l_svr) = self.server.write(self.width, d, True)
            t_s_w *= bsize / stripe
            t_s_r = 0   # no reads
            t_s_s = 0   # no setattrs
            reads = 0
            writes = (self.n + self.m) * bsize / stripe
            commits = self.n + self.m
            setattrs = 0
        elif seq:
            # small sequential writes get aggregated into stripes
            d = max(1, depth * bsize / stripe)
            (t_s_w, bw_svr, l_svr) = self.server.write(self.width, d, seq)
            t_s_w /= stripe / bsize
            t_s_r = 0   # no reads
            t_s_s = 0   # no setattrs
            reads = 0
            writes = self.n + self.m
            commits = self.n + self.m
            setattrs = 0
        else:
            # small random writes require read/modify/write!
            reads = self.n
            writes = 1 + self.m
            commits = 1 + self.m
            setattrs = self.n - 1
            (t_s_s, bw_svr, l_svr) = self.server.setattr()
            (t_s_r, bw_svr, l_svr) = self.server.read(self.width, depth, seq)
            (t_s_w, bw_svr, l_svr) = self.server.write(self.width, depth, seq)

            # cost of additional checksum computation
            t_cpu += (self.n - 1) * self.write_mult * self.cpu.process(bsize)

        # the time for a server to handle a commit is the same in all cases
        (t_s_c, bw_c, l) = self.server.commit()
        t_svr = t_s_r + t_s_w + t_s_s + t_s_c

        # what does this, in principle tell us about the cluster bandwidth
        #   NOTE: this is a highly theoretical number
        bw_svr *= self.num_servers * self.n / (self.n + self.m)

        # figure out the messages we will exchange with the servers
        t_back_w += reads * Lbw     # reads for strips to update
        t_cpu += reads * self.back.write_cpu(small)
        t_back_r += reads * LbR     # read responses to reads
        t_cpu += reads * self.back.read_cpu(large)
        t_back_w += writes * LbW    # writes of updated strips
        t_cpu += writes * self.back.write_cpu(large)
        t_back_r += writes * LbR    # read responses to writes
        t_cpu += writes * self.back.read_cpu(small)
        t_back_w += commits * Lbw   # writes of commits
        t_cpu += commits * self.back.write_cpu(small)
        t_back_w += setattrs * Lbw   # writes of setattrs
        t_cpu += setattrs * self.back.write_cpu(small)

        # cost of sending the response back to the client
        t_front_w = Lfw
        t_cpu += self.front.write_cpu(small)

        # compute available network bandwidth
        #	NICs are full duplex, but we only look at the side that
        #	sees the most traffic for the simulated operation
        #   (in this case, front-side reads and back-side writes)
        bw_nf = self.num_fronts * bsize * SECOND / t_front_r
        bw_nb = self.num_backs * bsize * SECOND / t_back_w

        # compute the available CPU bandwidth
        avail_cores = self.num_cpus * self.cpu.cores * self.cpu.hyperthread
        bw_cpu = avail_cores * bsize * SECOND / t_cpu

        # compute the request latency and throughputs
        latency = t_front_w + t_back_w + t_cpu + t_lock + t_svr
        bw_base = depth * bsize * SECOND / latency
        bandwidth = min(bw_base, bw_dlm, bw_svr, bw_nf, bw_nb, bw_cpu)
        iops = bandwidth / bsize
        q_delay = 0

        load = {}
        load['server'] = bandwidth / bw_svr
        load['dlm'] = bandwidth / bw_dlm

        # see what this means for front NIC load and queue
        if (bw_nf < bw_base):
            self.warn("Gateway front saturated by %dus x %d IOPS for %s\n" %
                      (t_front_r, iops, descr))
        nic_load = t_front_w * iops / float(self.num_fronts * SECOND)
        delay = t_front_w * self.front.queue_length(nic_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Gateway front load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (nic_load, delay, delta, descr))
        load['front'] = nic_load

        # see what this means for back NIC load and queue
        if (bw_nb < bw_base):
            self.warn("Gateway back saturated by %dus x %d IOPS for %s\n" %
                      (t_back_w, iops, descr))
        nic_load = t_back_w * iops / float(self.num_backs * SECOND)
        delay = t_back_w * self.back.queue_length(nic_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Gateway back load (%4.2f) adds %dus (%d%%) to %s\n" %
                      (nic_load, delay, delta, descr))
        load['back'] = nic_load

        # see what this means for CPU load and queue
        if (bw_cpu < bw_base):
            self.warn("Gateway CPUs saturated by %dus x %d IOPS for %s\n" %
                      (t_cpu, iops, descr))
        core_load = t_cpu * iops / float(avail_cores * SECOND)
        delay = t_cpu * self.cpu.queue_length(core_load, depth)
        q_delay += delay
        delta = 100 * float(delay) / latency
        if (delay >= WARN_DELAY and delta >= WARN_DELTA):
            self.warn("Gateway CPU (%4.2f) adds %dus (%d%%) to %s\n" %
                      (core_load, delay, delta, descr))
        load['cpu'] = core_load

        return (latency + q_delay, bandwidth, load)

    def create(self):
        """ creation one new data containing object on the data file system """

        return(1)   # LATER - implement create

    def delete(self):
        """ delete a data containing object from the data file system """

        return(1)   # LATER - implement delete
