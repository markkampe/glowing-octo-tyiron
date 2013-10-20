#
# nonesuch
#

"""
exerciser for a single server

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


def servertest(fs, depth=1, crtdlt=False,
               bsizes=(4096, 128 * 1024, 4096 * 1024)):
    """ compute & display standard test results """

    if crtdlt:
        tc = fs.create()
        td = fs.delete()

        r = Report(("create", "delete"))
        r.printHeading()
        r.printIOPS(1, (SECOND / tc, SECOND / td))
        r.printLatency(1, (tc, td))

    r = Report(("rnd read", "rnd write"))
    r.printHeading()
    for bs in bsizes:
        (trr, brr, rload) = fs.read(bs, depth=depth)
        (trw, brw, wload) = fs.write(bs, depth=depth)
        r.printBW(bs, (brr, brw))
        r.printIOPS(bs, (brr, brw))
        r.printLatency(bs, (trr, trw))
