#
# nonesuch
#

"""
exerciser for a single server
"""

from units import *


def servertest(fs, obj_size=4 * MEG, nobj=2500, depth=1, crtdlt=False):
    """ compute & display standard test results """

    if crtdlt:
        tc = fs.create()
        td = fs.delete()
        print("\t\t     create\t      delete")
        print("\t\t%6d IOPS\t %6d IOPS" % (iops(tc), iops(td)))
        print("")

    print("\t    bs\t    rnd read\t   rnd write")
    print("\t -----\t    --------\t   ---------")
    for bs in (4096, 128 * 1024, 4096 * 1024):
        (trr,brr) = fs.read(bs, depth=depth)
        (trw,brw) = fs.write(bs, depth=depth)

        format = "\t%5dK\t%7.1f MB/s\t%7.1f MB/s"
        print(format %
              (kb(bs), meg(float(brr)), meg(float(brw))))
        print("\t    \t %6d IOPS\t %6d IOPS" % (brr/bs, brw/bs))
        print("\t    \t %6d us\t %6d us\t(average latency)" % (trr, trw))
