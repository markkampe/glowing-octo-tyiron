#
# nonesuch
#

"""
exerciser for a single client talking to a cluster of servers

FIX:    The prime rule in simulations is to be very clear what
        we are trying to simulate.

        This should be simulating the I/O patterns coming from
        a real benchmark so that we can compare the simulated
        and actual results.  But for this particular simulation,
        I don't know what that benchmark would be.   As such
        it is hard to say whether or not we are simulating a
        representative I/O pattern or simulating/measuring
        it in a reasonable way.
"""

from Report import Report
from units import *


def gatewaytest(fs, depth=1, crtdlt=False):
    """ compute & display standard  test results """

    if crtdlt:
        tc = fs.create(depth=depth)
        td = fs.delete(depth=depth)

        r = Report(("create", "delete"))
        r.printHeading()
        r.printIOPS(1, (SECOND / tc, SECOND / td))
        r.printLatency(1, (tc, td))

    r = Report(("seq read", "seq write", "rnd read", "rnd write"))
    r.printHeading()
    for bs in (4096, 128 * 1024, 4096 * 1024):
        (tsr, bsr, usage) = fs.read(bs, depth=depth, seq=True)
        (tsw, bsw, usage) = fs.write(bs, depth=depth, seq=True)
        (trr, brr, usage) = fs.read(bs, depth=depth, seq=False)
        (trw, brw, usage) = fs.write(bs, depth=depth, seq=False)
        r.printBW(bs, (bsr, bsw, brr, brw))
        r.printIOPS(bs, (bsr, bsw, brr, brw))
        r.printLatency(bs, (tsr, tsw, trr, trw))
