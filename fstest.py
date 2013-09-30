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

from units import *
from report import Report


"""
file system simulation exerciser
"""


def fstest(fs, filesize=16 * MEG, depth=1, direct=False,
           sync=False, crtdlt=False):
    """ compute & display standard fio to filesystem on a disk
        fs -- file system to be tested
        filesize -- size of file in which I/O is being done
        depth -- number of concurrent requests
        direct -- I/O is direct (not buffered)
        sync -- updates are immediately flushed
    """

    if crtdlt:
        tc = fs.create(sync=sync)
        td = fs.delete(sync=sync)

        r = Report(("create", "delete"))
        r.printHeading()
        r.printIOPS(1, (SECOND / tc, SECOND / td))
        r.printLatency(1, (tc, td))

    r = Report(("seq read", "seq write", "rnd read", "rnd write"))
    r.printHeading()
    for bs in (4096, 128 * 1024, 4096 * 1024):
        tsr = fs.read(bs, filesize, seq=True, depth=depth, direct=direct)
        tsw = fs.write(bs, filesize, seq=True, depth=depth, direct=direct,
                       sync=sync)
        trr = fs.read(bs, filesize, seq=False, depth=depth, direct=direct)
        trw = fs.write(bs, filesize, seq=False, depth=depth, direct=direct,
                       sync=sync)

        # compute the corresponding bandwidths
        bsr = bs * SECOND / tsr
        bsw = bs * SECOND / tsw
        brr = bs * SECOND / trr
        brw = bs * SECOND / trw

        r.printBW(bs, (bsr, bsw, brr, brw))
        r.printIOPS(bs, (bsr, bsw, brr, brw))
        r.printLatency(bs, (tsr, tsw, trr, trw))
