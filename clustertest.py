#
# nonesuch
#

"""
Clustered storage system emulator
"""

from units import *


def clustertest(fs, obj_size=16 * MEG, nobj=2500,
                clients=1, depth=1, copies=1, crtdlt=False):
    """ compute & display standard  test results """

    if crtdlt:
        tc = fs.create(depth=depth)
        td = fs.delete(depth=depth)
        print("\t\t     create\t      delete")
        print("\t\t%6d IOPS\t %6d IOPS" % (iops(tc), iops(td)))
        print("")

    print("\t    bs\t    rnd read\t   rnd write")
    print("\t -----\t    --------\t   ---------")
    for bs in (4096, 128 * 1024, 4096 * 1024):
        trr = fs.read(bs, obj_size, nobj=nobj, clients=clients, depth=depth)
        trw = fs.write(bs, obj_size, nobj=nobj, depth=depth,
                       clients=clients, copies=copies)

        format = "\t%5dK\t%7.1f MB/s\t%7.1f MB/s"
        print(format %
              (kb(bs), bw(bs, float(trr)), bw(bs, float(trw))))
        print("\t    \t %6d IOPS\t %6d IOPS" % (iops(trr), iops(trw)))
