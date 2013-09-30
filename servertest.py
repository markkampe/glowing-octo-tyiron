#
# nonesuch
#

"""
exerciser for a single server
"""

from Report import Report
from units import *


def servertest(fs, obj_size=4 * MEG, depth=1, crtdlt=False):
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
    for bs in (4096, 128 * 1024, 4096 * 1024):
        (trr, brr) = fs.read(bs, depth=depth)
        (trw, brw) = fs.write(bs, depth=depth)
        r.printBW(bs, (brr, brw))
        r.printIOPS(bs, (brr, brw))
        r.printLatency(bs, (trr, trw))
