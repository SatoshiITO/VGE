#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
from mpi4py import MPI
import time

def send_emergencysignal(comm, nproc, master_rank, message_tag):
    #
    command="killthejob"
    #
    list_signal=[-1]*nproc
    list_check=[-1]*nproc
    #
    sleep_time = 0.001 # sec ; pause after isend is performed.
    #
    # send a signal 
    #
    for worker_rank in range(nproc):
        if worker_rank is not master_rank:
           list_signal[worker_rank]=comm.isend(command, dest=worker_rank, tag=message_tag)
           #print "command.... in send_emergencysignal", command
           #list_signal[worker_rank]=comm.isend("aaaa", dest=worker_rank, tag=message_tag)
           list_check[worker_rank] = 1
           #
           time.sleep(sleep_time)

    #
    # wait a moment
    # 
    # time.sleep(1)
    #
    # check a signal whether worker receives or not.
    #
    #max_count=10
    #
    #for worker_rank in range(nproc):
    #   if worker_rank is not master_rank:
    #      reply = False
    #      flag=True
    #      icount=0
    #      while max_count > icount:
    #         if not list_signal[worker_rank].Test():
    #            reply = list_signal[worker_rank].Wait()
    #            list_check[worker_rank] = icount
    #            break
    #         else:
    #            time.sleep(sleep_time)
    #            icount += 1
    #
    #
    return list_signal, list_check
    #
    #


