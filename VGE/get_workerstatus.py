#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
from mpi4py import MPI
import time

def get_workerstatus(comm, target_worker_rank, number_of_probe, nsleep_probe):
    #
    flag_probe =False
    #
    for kk in xrange(number_of_probe):
        #
        flag_probe = comm.iprobe(source=target_worker_rank, tag=MPI.ANY_TAG, status=None)
        #
        if not flag_probe:
             time.sleep(nsleep_probe)
        else:
             return flag_probe
    #
    #print flag_probe
    return flag_probe


