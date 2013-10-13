#
# nonesuch
#

"""
This is a simulation of a single protocol gateway server that
stripes (and erasure codes) its data across multiple storage
servers.
"""

from units import KB, SECOND

WARN_LEVEL = 0.8            # max expected load on pass-through resources


class Gateway:
    """ Performance Modeling Gateway Server Simulation. """

    warnings = ""           # save these up for reporting later

    def __init__(self,
                 server,
                 front_nic,
                 back_nic,
                 cpu,
                 num_servers=1,
                 num_front=1,
                 num_back=1,
                 num_cpus=1,
                 n=5,
                 m=2,
                 stripe=128 * KB):

        """ create a Gateway server simulation
            server -- simulation for the file server nodes
            nic -- SimIFC for the network interface
            cpu -- SimCPU for the processor
            num_servers -- number of file servers
            num_nic -- number of NICs per server
            num_cpus -- number of processors per server
            n -- number of data blocks in a stripe
            m -- number of parity blocks in a stripe
            stripe -- width of a stripe
        """
        self.server = server
        self.front = front_nic
        self.back = back_nic
        self.cpu = cpu
        self.num_servers = num_servers  # FIX ... unimplemented
        self.num_fronts = num_front
        self.num_backs = num_back
        self.num_cpus = num_cpus
        self.n = n
        self.m = m
        self.width = stripe

        # magic constants
        self.min_msg = 128      # size of a minimal request/rsponse
        self.read_mult = 2      # multipler on read request processing
        self.write_mult = 3     # multipler on write request processing

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
        Lfr = self.front.min_read_latency
        Lfw = self.front.min_write_latency
        Lbr = self.back.min_read_latency
        Lbw = self.back.min_write_latency
        req = self.min_msg
        rsp = self.min_msg + bsize

        # cost of receiving and processing original request
        t_net = Lfr + self.front.read_time(req)
        t_cpu = self.front.read_cpu(req)

        # cost of obtaining the necessary lock
        P_lock = 0              # FIX ... how often to dw need lock
        t_net += P_lock * (Lbw + self.back.write_time(req))
        t_net += P_lock * (Lbw + self.back.read_time(req))
        t_cpu += P_lock * (self.back.read_cpu(req))
        t_cpu += P_lock * (self.back.write_cpu(req))
        t_lock = P_lock * 0     # FIX ... probably need a DLM model

        # cost of forwarding request to servers and awaiting response(s)
        sseq = False            # CLAIM: striping breaks sequential reads
        if bsize <= self.width:
            # FIX do we do full stripe reads?
            (t_svr, bw_svr, l_svr) = self.server.read(bsize, depth, sseq)
            t_cpu += self.back.write_cpu(req)
            t_cpu += self.back.read_cpu(rsp)
        else:
            shards = (bsize + self.width - 1) / self.width
            shard_size = self.width
            (t_svr, bw_svr, l_svr) = self.server.read(self.width, depth, sseq)
            t_svr *= shards         # FIX - this ignores large read parallelism
            t_cpu += shards * self.back.write_cpu(req)
            t_cpu += shards * self.back.read_cpu(self.min_msg + shard_size)

        # CPU time to process actually process the data
        t_cpu = self.read_mult * self.cpu.process(bsize)

        # cost of sending the response back to the client
        t_net += Lfw + self.front.write_time(rsp)           # send response
        t_cpu += self.front.write_cpu(rsp)

        delay = 0  # FIX - compute lock server queueing delays
        if (delay > 0):
            self.warnings += "DLM load adds %dus to %s\n" % \
                (delay, descr)
            t_queue += delay

        delay = 0  # FIX - compute CPU queueing delays
        if (delay > 0):
            self.warnings += "Gateway CPU load adds %dus to %s\n" % \
                (delay, descr)

        delay = 0  # FIX - compute back-side queueing delays (to all servers)
        if (delay > 0):
            self.warnings += "Gateway back-NIC load adds %dus to %s\n" % \
                (delay, descr)

        delay = 0  # FIX - compute front-side queueing delays
        if (delay > 0):
            self.warnings += "Gateway front-NIC load adds %dus to %s\n" % \
                (delay, descr)

        # compute the effective network and CPU bandwidth
        bw_nf = self.num_fronts * self.front.max_write_bw
        bw_nb = self.num_backs * self.back.max_read_bw
        bw_cpu = bsize * SECOND / t_cpu
        bw_svr *= self.num_servers    # FIX this ignores long write stripe wrap

        # compute the request latency and throughputs
        latency = t_net + t_cpu + t_lock + t_svr + t_queue
        iops = depth * SECOND / latency
        bandwidth = min(bsize * iops, bw_nf, bw_nb, bw_cpu)
        load = {
            'front': bandwidth / bw_nf,
            'back': bandwidth / bw_nb,
            'cpu': bandwidth / bw_cpu,
            'server': bandwidth / bw_svr,
            'dlm': 0,                   # FIX - get from DLM model
        }

        # check for unexpected throughput caps
        for key in ('front', 'back', 'cpu', 'dlm'):
            l = load[key]
            if l > 0.99:
                self.warnings += \
                    "Gateway throughput is capped by %s for %s\n" \
                    % (key, descr)
            elif l >= WARN_LEVEL:
                self.warnings += "Gateway %s load at %4.2f for %s\n" % \
                    (key, l, descr)

        return (latency, bandwidth, load)

    def write(self, bsize, depth=1, seq=False):
        """ expected write performance
            bsize -- size of each request
            depth -- number of parallel requests (multiple objects)
            seq -- is this sequential I/O (subsequent writest to same objs)
        """

        descr = "%dK, d=%d %s writes" % \
            (bsize / 1024, depth, "seqential" if seq else "random")

        # we start out with no queuing delays
        t_queue = 0

        # network times for request receipt and response transmission
        Lfr = self.front.min_read_latency
        Lfw = self.front.min_write_latency
        Lbr = self.back.min_read_latency
        Lbw = self.back.min_write_latency
        req = self.min_msg + bsize
        rsp = self.min_msg

        # cost of receiving and processing original request
        t_net = Lfr + self.front.read_time(req)
        t_cpu = self.front.read_cpu(req)

        # cost of obtaining the necessary lock
        P_lock = 0              # FIX ... how often to dw need lock
        t_net += P_lock * (Lbw + self.back.write_time(req))
        t_cpu += P_lock * (self.back.write_cpu(req))
        t_lock = P_lock * 0     # FIX ... probably need a DLM model
        t_net += P_lock * (Lbw + self.back.read_time(req))
        t_cpu += P_lock * (self.back.read_cpu(req))

        # CPU time to process actually process the data
        t_cpu = self.write_mult * self.cpu.process(bsize)

        # cost of forwarding request to servers and awaiting response(s)
        sseq = seq      # CLAIM: sequential writes eventually aggregate on svr
        if bsize == self.width:     # write one data strip, two parity strips
            shards = 1 + self.m         # we write data and parity
            # count network time for all but the last shard
            t_net += (shards - 1) * self.back.write_time(req)
            t_net += (shards - 1) * self.back.read_time(rsp)
            t_cpu += shards * self.back.write_cpu(req)
            t_cpu += shards * self.back.read_cpu(rsp)
            (t_svr, bw_svr, l_svr) = self.server.write(bsize, depth, sseq)
        elif bsize > self.width:    # write multiple data + 2 parity
            shards = (bsize + self.width - 1) / self.width
            shards += self.m            # FIX - ignores long stripe wrap
            sz = self.min_msg + self.width
            # count network time for all but the last shard
            t_net += (shards - 1) * self.back.write_time(sz)
            t_net += (shards - 1) * self.back.read_time(rsp)
            t_cpu += shards * self.back.write_cpu(sz)
            t_cpu += shards * self.back.read_cpu(rsp)
            (t_svr, bw_svr, l_svr) = self.server.write(self.width, depth, sseq)
        else:       # read/modify/write one data strip + 2 parity strips
            shards = 1 + self.m
            sz = self.min_msg + self.width
            # count network time for all but the last shard
            t_net += (shards - 1) * self.back.write_time(self.min_msg)
            t_net += (shards - 1) * self.back.read_time(sz)
            t_net += (shards - 1) * self.back.write_time(sz)
            t_cpu += shards * self.back.write_cpu(self.min_msg)
            t_cpu += shards * self.back.read_cpu(sz)
            t_cpu += shards * self.back.write_cpu(sz)
            (t_sr, bw_sr, l_svr) = self.server.read(bsize, depth, sseq)
            (t_sw, bw_sw, l_sw) = self.server.write(self.width, depth, sseq)
            t_svr = t_sr + t_sw
            bw_svr = (bw_sr + bw_sw) / 2

        # cost of acking the commit to the servers
        t_net += shards * (Lbw + self.back.write_time(self.min_msg))
        t_cpu += shards * self.back.write_cpu(self.min_msg)

        # cost of sending the response back to the client
        t_net += Lfw + self.front.write_time(rsp)
        t_cpu += self.front.write_cpu(rsp)

        # compute the effective network and CPU bandwidth
        bw_nf = self.num_fronts * self.front.max_write_bw
        bw_nb = self.num_backs * self.back.max_read_bw
        bw_cpu = bsize * SECOND / t_cpu
        bw_svr *= self.num_servers    # FIX this ignores long write stripe wrap

        # compute the request latency and throughputs
        latency = t_net + t_cpu + t_lock + t_svr + t_queue
        iops = depth * SECOND / latency
        bandwidth = min(bsize * iops, bw_nf, bw_nb, bw_cpu)
        load = {
            'front': bandwidth / bw_nf,
            'back': bandwidth / bw_nb,
            'cpu': bandwidth / bw_cpu,
            'server': bandwidth / bw_svr,
            'dlm': 0,                   # FIX - get from DLM model
        }

        # check for unexpected throughput caps
        for key in ('front', 'back', 'cpu', 'dlm'):
            l = load[key]
            if l > 0.99:
                self.warnings += \
                    "Gateway throughput is capped by %s for %s\n" % \
                    (key, descr)
            elif l >= WARN_LEVEL:
                self.warnings += "Gateway %s load at %4.2f for %s\n" % \
                    (key, l, descr)

        return (latency, bandwidth, load)

    def create(self):
        """ creation one new data containing object on the data file system """

        return(1)   # FIX - impleemnt create

    def delete(self):
        """ delete a data containing object from the data file system """

        return(1)   # FIX - impleemnt delete
