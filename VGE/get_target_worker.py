from mpi4py import MPI
import time

from VGE.get_workerstatus import get_workerstatus

def get_target_worker(comm, nproc,  master_rank, sorted_node_list, worker_wait_list, number_of_probe, nsleep_probe, target_flag):
    #
    found_worker=False
    target_worker_rank = 0
    jj = 0
    if target_flag in worker_wait_list.values():#
        for jj in range(nproc):
            if jj != master_rank:
                  target_worker_rank = sorted_node_list[jj][0]
                  if worker_wait_list[target_worker_rank] is target_flag:
                          flag_probe = get_workerstatus(comm, target_worker_rank, number_of_probe, nsleep_probe)
                          if flag_probe:
                             found_worker=True
                             return found_worker, target_worker_rank, jj

    return found_worker, target_worker_rank, jj



