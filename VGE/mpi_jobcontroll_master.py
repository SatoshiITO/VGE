from mpi4py import MPI
import os,math
import multiprocessing
import select, socket, pickle
import time, datetime

from VGE.get_workerstatus import get_workerstatus
from VGE.get_target_worker import get_target_worker
from VGE.get_vge_conf import get_vge_conf


def mpi_jobcontroll_master(cl_args, mpi_args, comm, total_joblist,new_joblist,task_check,command_list):   

    from logging import getLogger,StreamHandler,basicConfig,DEBUG,INFO,WARNING,ERROR,CRITICAL
    logger=getLogger(__name__)
    logger.setLevel(INFO)

    #
    # set verbose level
    #
    verbose = 0 
    verbose = int(get_vge_conf("vge","verbose",verbose))
    if verbose == 0:
        logger.setLevel(CRITICAL)
    elif verbose == 1 or verbose == 2:
        logger.setLevel(INFO)
    elif verbose == 3:
        logger.setLevel(DEBUG)
    basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')

    #
    # 
    #
    myrank = comm.Get_rank()
    nproc = comm.Get_size()
    number_of_nproc_digit= int(math.log10(nproc)+1)
    flag_found_target=False
    #
    mypid = os.getpid()
    logger.info("VGE(MPI): MPI job controller (master [%i]) [pid:%i] is running." %(myrank,mypid))
    #
    status=MPI.Status()
    command = None
    worker_jobstatus={}
    number_of_jobs =len(total_joblist)
    current_joblist={}
    master_rank = 0
    thresh_count=nproc
    still_have_jobs=True
    probe_time = 0.0
    time_sort_list =0.0

    #
    # load balancer switch
    #
    flag_worktime_loadbalancer=False
    flag_jobcount_loadbalancer=False
    if cl_args.loadbalancer == "time":
       flag_worktime_loadbalancer=True 
    elif cl_args.loadbalancer == "count":
       flag_jobcount_loadbalancer=True

    #
    # load balancer 
    #
    worker_jobcount_list = {}
    worker_worktime_list = {}
    worker_wait_list = {}
    for ii in range(nproc):
       worker_jobcount_list[ii] = 0
       worker_worktime_list[ii] = 0
       worker_wait_list[ii] = None
    worker_jobcount_list[master_rank] = -1
    worker_worktime_list[master_rank] = -1
    worker_wait_list[master_rank] = "master"
    check_worker=""
    #
    flag_loadbalancer=False
    if flag_jobcount_loadbalancer or flag_worktime_loadbalancer:
        flag_loadbalancer=True
        if flag_worktime_loadbalancer:
           if flag_jobcount_loadbalancer == flag_worktime_loadbalancer:
               flag_jobcount_loadbalancer=False # priority: worktime > jobcount 
        if flag_jobcount_loadbalancer:
           logger.info("VGE(MPI): load balancer mode ==> job count")
        elif flag_worktime_loadbalancer:
           logger.info("VGE(MPI): load balancer mode ==> work time")
    else:
        logger.info("VGE(MPI): load balancer mode ==> off (any worker)")
    default_flag_loadbalancer=flag_loadbalancer
    change_loadbalancer=False

    # 
    # config
    #

    # worker probe parameters
    number_of_probe = 10
    nsleep_probe=0.0001
    # wait time
    nsleep_updatelist=0.000

    # read from conf
    if mpi_args["flag_vge_conf"]:
       number_of_probe = int(get_vge_conf("vge","mpi_num_probe",number_of_probe))
       nsleep_probe = get_vge_conf("vge","mpi_interval_probe", nsleep_probe)
       nsleep_updatelist= get_vge_conf("vge","mpi_interval_update", nsleep_updatelist)
    logger.info("VGE(MPI): mpi_interval_probe=%12.5e" %nsleep_probe)
    logger.info("VGE(MPI): mpi_num_probe=%i" %number_of_probe)
    logger.info("VGE(MPI): mpi_interval_update=%12.5e" %nsleep_updatelist)

    # flag write csv file
    flag_write_csvfile=True
    if cl_args.nowrite_vgecsv:
        flag_write_csvfile=False
    
    #recv_list=[None]*nproc
    recv_list=[]

    #
    # probe test 
    #
    if cl_args.check_mpi_probe:
        logger.info("VGE(MPI): probing workers.....")
        time_probe_test=time.time()
        worker_probe_list=[0] *nproc
        worker_probe_list[master_rank] = -1
        max_probe_trial =  number_of_probe * 10
        #
        for ii in range(1,nproc):
            for jj in range(max_probe_trial):
                flag = comm.iprobe(source=ii, tag=MPI.ANY_TAG, status=None)
                if flag: 
                    worker_probe_list[ii] = (jj+1)
                    break
                else:
                    time.sleep(nsleep_probe)
                    pass
        time_probe_test=time.time() - time_probe_test
        #
        logger.info("VGE(MPI): .....done. [%8.3e sec]" %time_probe_test)
        logger.info("VGE(MPI): MPI probe test for %i workers --> probed 1:[%i] 20>:[%i] >=20:[%i] not probed:[%i] (max. trial:%i)" \
               %(nproc-1, worker_probe_list.count(1), \
               len([1 for ii in worker_probe_list if 1 < ii and ii <20]), \
               len([1 for ii in worker_probe_list if ii >=20 ]), \
               worker_probe_list.count(0), max_probe_trial))
        logger.debug("VGE(MPI): worker probe count list:")
        logger.debug("VGE(MPI): "+ ''.join('{}:{}|'.format(*k) for k in enumerate(worker_probe_list))) 

    #
    #
    #

    #//////////////////////////
    # job loop for the master node
    #//////////////////////////
    while still_have_jobs:
       #
       #logger.info("VGE(MPI): worker_jobcount_list (%s) " %worker_jobcount_list)

       if default_flag_loadbalancer is not flag_loadbalancer and command is not None: 
           logger.debug("default_flag_loadbalancer =%s flag_loadbalancer=%s" %(default_flag_loadbalancer,flag_loadbalancer))
           if flag_jobcount_loadbalancer:
              logger.info("VGE(MPI): change load balancer -> job count")
              pass
           elif flag_worktime_loadbalancer:
              logger.info("VGE(MPI): change load balancer -> work time")
              pass
           flag_loadbalancer=default_flag_loadbalancer
       #
       start_job = time.time()
       #
       # 1.make a new job to send.
       # 2.until a worker is free and responses to the master,
       # the master continues to check new jobs from genomon job controller, 
       # and receive new jobs from the job controller during checking.
       #
       found_count=0 
       flag_new_sendjob=False

       #
       # job to be send
       #
       completed_newjob_package=False
       jobid = None
       command = None 
       command_id = None
       filename = None
       package={}
       package["command"] = command
       package["filename"] = filename
       package["jobid"] = jobid
       #
       search_target_worker=True
       while search_target_worker:
           #
           if len(new_joblist) != 0:  # got new jobs
               logger.info( "VGE(MPI): got new job(s) from VGE(JOB)")
               #if cl_args.debug:
                   #temp = new_joblist.copy()
                   #import pprint
                   #pp = pprint.PrettyPrinter(indent=4)
                   #logger.debug("VGE(MPI): new job contents: %s"  %pp.pprint(temp))
                   #del temp
               logger.debug("VGE(MPI): new job contents: %s"  %new_joblist)
               for newjobid, newjob in new_joblist.items():
                   if newjob.has_key("status") is True:
                       if  newjob["status"] is not None: 
                           #jobid = number_of_jobs  # this is because the jobid starts from zero.
                           current_joblist[newjobid] = newjob
                           total_joblist[newjobid] = newjob
                           number_of_jobs +=1 # update maximum number of jobs so far
                           del new_joblist[newjobid]  # delete this order because we have already received
                           time.sleep(nsleep_updatelist)
               #logger.debug("VGE(MPI): update total joblist: %s"  %total_joblist)
               #logger.debug("VGE(MPI): update current_joblist: %s"  %current_joblist)
               #
               if command == None and completed_newjob_package:
                   completed_newjob_package=False

           # check recv signal from workers
           #probe_time = time.time()
           #for ii in range(nproc):
           #    if ii != master_rank:
           #        recv_list[ii] =get_workerstatus(comm, ii, number_of_probe, nsleep_probe)
           #probe_time = time.time() - probe_time

           #
           # make a new job
           #
           if not completed_newjob_package :
               jobid = None
               command = None 
               command_id = None
               filename = None
               if len(current_joblist) != 0:
                   for jobid_temp, job in current_joblist.items():
                       if job["sendtoworker"] == False: 
                            #
                            jobid = jobid_temp
                            filename=job["filename"]
                            command_id = job["command_id"]
                            bulkjob_id = job["bulkjob_id"]

                            # update current_joblist
                            current_joblist[jobid]["sendtoworker"] = True 

                            # replace VGE_BULKJOB_ID in the command with bludjob_id 
                            temp = ""
                            temp = command_list[command_id]["command"]
                            command = temp.replace("VGE_BULKJOB_ID", str(bulkjob_id))
                            del temp

                            # update total_joblist
                            if total_joblist.has_key(jobid):
                               temp = total_joblist[jobid]
                               temp["sendtoworker"] = True
                               total_joblist[jobid]=temp
                               del temp
                               time.sleep(nsleep_updatelist)
                               break # escape from this loop.
               #
               package={}
               package["command"] = command
               package["filename"] = filename
               package["jobid"] = jobid
               completed_newjob_package=True


           if task_check.has_key("exit"):
               if task_check["exit"]:
                   flag_loadbalancer=False
                   change_loadbalancer=True
                   search_target_worker=False
                   logger.info("VGE(MPI): set load balancer -> any worker")
                   break
           #
           if flag_loadbalancer:
               #
               # get a target worker 
               #
               if len(current_joblist) > 0:
                   # check
                   target_worker_rank = -1
                   found_worker=False
                   jj = 0

                   #
                   if completed_newjob_package:
                       if None in worker_wait_list.values():
                           for rank_temp, check_flag in worker_wait_list.items():
                               if check_flag is None:
                                   jj += 1
                                   search_target_worker=False
                                   target_worker_rank = rank_temp
                                   flag_found_target=True
                                   found_worker=True
                                   break #exit this loop
                   if found_worker:
                       check_worker="found target worker[%s]: 1st(None) -- jj(%i) sort(%8.3e)" %(target_worker_rank,jj,time_sort_list)
                       search_target_worker=False
                       break

                   #
                   # search a target worker rank
                   #
                   if not found_worker: 
                       #
                       # Do we have new job(s) to send?
                       # 
                       time_sort_list =time.time()
                       if flag_jobcount_loadbalancer:
                           sorted_node_list = sorted(worker_jobcount_list.items(), key = lambda x:x[1])
                           #logger.debug("node_joboucnt_list (%s)" %worker_jobcount_list)
                           #logger.debug("sorted_node_joboucnt_list (%s)" %sorted_node_list)
                       elif flag_worktime_loadbalancer:
                           sorted_node_list = sorted(worker_worktime_list.items(), key = lambda x:x[1])
                           #logger.debug("worker_worktime_list (%s)" %worker_worktime_list)
                           #logger.debug("sorted_node_joboucnt_list (%s)" %sorted_node_list)
                       time_sort_list =time.time() - time_sort_list

                       flag_new_sendjob=False
                       for jobid_temp, job in current_joblist.items():
                           if job["sendtoworker"] == False: 
                               flag_new_sendjob=True
                               break
                       #
                       check_priority=True
                       if flag_new_sendjob:
                           found_worker, target_worker_rank, jj = \
                                 get_target_worker(comm, nproc,  master_rank, sorted_node_list, \
                                                   worker_wait_list, number_of_probe, nsleep_probe, False)
                           if found_worker: 
                               check_priority=True
                           else: 
                               check_priority=False
                               found_worker, target_worker_rank, jj = \
                                    get_target_worker(comm, nproc,  master_rank, sorted_node_list, \
                                                 worker_wait_list, number_of_probe, nsleep_probe, True)
                       else:
                           found_worker, target_worker_rank, jj = \
                               get_target_worker(comm, nproc,  master_rank, sorted_node_list, \
                                              worker_wait_list, number_of_probe, nsleep_probe, True)
                           if found_worker:
                               check_priority=True
                           else: 
                               check_priority=False
                               found_worker, target_worker_rank, jj = \
                                   get_target_worker(comm, nproc,  master_rank, sorted_node_list, \
                                                 worker_wait_list, number_of_probe, nsleep_probe, False)
                       if check_priority:
                           check_worker="found target worker[%s]: 1st -- jj(%i) sort(%8.3e)" %(target_worker_rank,jj,time_sort_list)
                       else:
                           check_worker="found target worker[%s]: 2nd -- jj(%i) sort(%8.3e)" %(target_worker_rank,jj,time_sort_list)
                       #
                       if found_worker:
                           search_target_worker=False
                           break

               else:  # since current job is empty, load balancer will be turned off.
                   if task_check.has_key("exit"):
                       if task_check["exit"]:
                           logger.debug("VGE(MPI): change load balancer switch -> any worker")
                   change_loadbalancer=True
                   search_target_worker=False
                   break

           #
           # load balancer off 
           #
           else: 
               # check
               target_worker_rank = -1

               # 1st priority
               if completed_newjob_package:
                   if None in worker_wait_list.values():
                       for rank_temp, check_flag in worker_wait_list.items():
                           if check_flag is None:
                               search_target_worker=False
                               target_worker_rank = rank_temp
                               flag_found_target=True
                               found_worker=True
                               logger.debug("VGE(MPI): found target worker[%s]: None / any worker" %(target_worker_rank))
                               break #exit this loop

               # 2nd priority
               flag_new_sendjob=False
               if not found_worker and len(current_joblist) > 0:
                   flag_new_sendjob=False
                   for jobid_temp, job in current_joblist.items():
                       if job["sendtoworker"] == False: 
                           flag_new_sendjob=True
                           break #exit this loop
               if not flag_new_sendjob and not found_worker:
                   for jobid_temp, job in current_joblist.items():
                       if job["sendtoworker"] == True and job["status"] == "wait":
                           target_worker_rank = int(job["worker"])
                           flag_probe = get_workerstatus(comm, target_worker_rank, number_of_probe, nsleep_probe)
                           if flag_probe:
                               flag_found_target=True
                               logger.debug("VGE(MPI) ... get job status from worker[%i]" %target_worker_rank)
                               search_target_worker=False
                               break 
               else:
                   # check a signal from a worker
                   flag_probe = get_workerstatus(comm, MPI.ANY_SOURCE, number_of_probe, nsleep_probe)
                   if flag_probe:
                       search_target_worker=False
                       break 
                           
       #
       # check load balancer switch
       #
       if flag_loadbalancer:
           if change_loadbalancer == True:
              flag_loadbalancer=False
              change_loadbalancer=False


       #
       # recieve an inquiry from a worker. 
       #
       # the inquiry includes previous job status for this worker.
       dispatch_time =time.time()
       start_recv=time.time()
       #logger.debug("master:check before recv1")
       if flag_loadbalancer or flag_found_target:
           worker_jobstatus = comm.recv(source=target_worker_rank, tag=MPI.ANY_TAG, status=status)
           flag_found_target=False
       else:
           worker_jobstatus = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
             
       #logger.debug("master:check after recv1")
       end_recv=time.time()
       mpi_recvtime = end_recv - start_recv
       #
       worker_source = status.Get_source() # the worker rank number called
       worker_tag = status.Get_tag()
       #logger.debug("master:check get worker_source (%i)" %worker_source)

       #
       # send the job to this worker
       #
       if command is not None:
            logger.info("VGE(MPI): send job [id:%i] to worker[%i]: command[%s]" %(jobid, worker_source, command))
            pass
       if command is None and jobid is None:
            jobid = worker_source  # if command is empty so jobid will be assigned to the worker rank to avoid an error
       start_send=time.time()
       #logger.debug("master:check before send")
       comm.send(package, dest=worker_source, tag=jobid)
       #logger.debug("master:check after send")
       end_send=time.time()
       mpi_sendtime = end_send - start_send
       dispatch_time = time.time() - dispatch_time

       #
       # check the previous job result from this worker
       #
       # if woker_jobstatus["status"] = none then this worker did not DO any job.
       # worker_jobstatus["jobid"] is the job id number that the worker did work just before.
       if worker_jobstatus is not None :
         if worker_jobstatus["jobid"] is not None: 
            #
            # update job-status.
            #
            if current_joblist.has_key(worker_jobstatus["jobid"]):
               if current_joblist[worker_jobstatus["jobid"]].has_key("status"):
                   current_joblist[worker_jobstatus["jobid"]]["status"] = worker_jobstatus["status"]
            #
            if total_joblist.has_key(worker_jobstatus["jobid"]):
               temp = total_joblist[worker_jobstatus["jobid"]]
               if temp.has_key("status"):
                   temp["status"] = worker_jobstatus["status"]
                   temp["start_time"] = worker_jobstatus["start_time"]
                   temp["finish_time"] = worker_jobstatus["finish_time"]
                   temp["elapsed_time"] = worker_jobstatus["elapsed_time"]
                   temp["return_code"] = worker_jobstatus["return_code"]
                   total_joblist[worker_jobstatus["jobid"]] = temp
                   time.sleep(nsleep_updatelist)

                   logger.info("VGE(MPI): worker[%i] job status = %s"  %(worker_source, worker_jobstatus))
                   if worker_jobstatus["return_code"] != 0:
                       logger.warning("VGE(MPI): job [id %i] returned with code(%i)" \
                          %(worker_jobstatus["jobid"], worker_jobstatus["return_code"]))
            # 
            worker_worktime_list[worker_source] += float(worker_jobstatus["elapsed_time"])

       #
       # update woker jobcount/wait lists
       #
       if command is not None: 
           worker_jobcount_list[worker_source] += 1
           worker_wait_list[worker_source] = True
           if flag_loadbalancer:
               if recv_list != []:
                   logger.debug("worker_source(%i) recv_list: %s" %(worker_source,recv_list))
                   logger.debug("probe_time: %8.3e sec" %probe_time)
               logger.debug("check_worker: %s" %check_worker)
               logger.debug("flag_new_sendjob:   %s" %flag_new_sendjob)
           logger.debug("worker_wait_list:   %s" %worker_wait_list)
           logger.debug("worker_jobcount_list: %s" %worker_jobcount_list)
           logger.debug("worker_worktime_list: %s" %worker_worktime_list)
       else:
           worker_wait_list[worker_source] = False
       if default_flag_loadbalancer: 
            flag_loadbalancer=True

       #
       # change the job status to "wait" and mark the worker rank for the job that be send.
       #
       if len(current_joblist) != 0:
           if command is not None and jobid is not None:
                current_joblist[jobid]["status"] = "wait"
                current_joblist[jobid]["worker"] = worker_source # this is a worker rank number.
                #
                # update
                temp = total_joblist[jobid]
                temp["status"] = "wait"
                temp["worker"] = worker_source
                total_joblist[jobid]=temp
                time.sleep(nsleep_updatelist)
                del temp

                #
       #logger.debug("VGE(MPI): %s"  %current_joblist)
                       
       #
       # delete finished job keys in current_joblist
       #
       for jobid_temp, job in current_joblist.items():
           if job["status"] == "done" and job["sendtoworker"] == True:
                del current_joblist[jobid_temp]
        
       #
       # whether all jobs are done or not
       #
       if command is None :
          if len(current_joblist) == 0:
             Final_flag = True
             for jobid_temp, job in total_joblist.items():
                 if job["status"] !=  "done":
                     Final_flag = False  # still have jobs...
                     #
             if Final_flag == True: 
                 # whehter VGE JOB_controller also wants to finish or not...
                 if task_check.has_key("exit"):
                     if task_check["exit"]==True:
                         still_have_jobs=False # all the jobs to be performed were done!
                         #logger.debug("VGE(MPI): %s"  %current_joblist)
                         #logger.debug("VGE(MPI): %s"  %total_joblist)
                         logger.info( "VGE(MPI): VGE MPI master controller finished normally.")
       else:
          time_job = time.time() - start_job
          if time_sort_list > 0.0 :
              logger.info("VGE(MPI): job [id:%d] dispatch -> worker [%i]: communication elapsed time -> mesg-recv[%8.3e]/send[%8.3e] sort[%8.3e] job-dispatch[%8.3e] 1-cycle[%8.3e] sec" \
                       %(jobid, worker_source, end_recv-start_recv, end_send-start_send, time_sort_list, dispatch_time, time_job))
          else:
              logger.info("VGE(MPI): job [id:%d] dispatch -> worker [%i]: communication elapsed time -> mesg-recv[%8.3e]/send[%8.3e] job-dispatch[%8.3e] 1-cycle[%8.3e] sec" \
                       %(jobid, worker_source, end_recv-start_recv, end_send-start_send, dispatch_time, time_job))

    #
    # finalize
    #
    pass
    logger.info("VGE(MPI): finalize...")
    nproc = comm.Get_size()
    for i in range(nproc-1):
        workrer_jobstatus= comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
        worker_source = status.Get_source()
        worker_tag = status.Get_tag()
        logger.info("VGE(MPI): from master [%i]: shutdown MPI worker [%i] " %(myrank, worker_source))
        command = "terminate_all_workers" # 
        comm.send(command, dest=worker_source, tag=worker_tag)

    #
    #
    #
    logger.debug("VGE(MPI): flag: load balancer ---------> %s" %flag_loadbalancer)
    logger.debug("VGE(MPI): flag: load balancer [worktime]:%s" %flag_worktime_loadbalancer)
    logger.debug("VGE(MPI): flag: load balancer [jobcount]:%s" %flag_jobcount_loadbalancer)

    #
    # write worker job status lists in json format
    #
    import json
    logger.debug("\nVGE(MPI): worker results [job count]:")
    logger.debug(json.dumps(worker_jobcount_list,indent=1))
    logger.debug("\nVGE(MPI): worker results [work time]:")
    logger.debug(json.dumps(worker_worktime_list,indent=1))
    logger.debug("VGE(MPI): VGE MPI controller finished normally.\n")

    #
    # write csv formatted file.
    #
    if not cl_args.nowrite_vgecsv:
        from VGE.write_vge_result1 import write_vge_result1
        from VGE.write_vge_result2 import write_vge_result2
        from VGE.write_vge_result3 import write_vge_result3
        temp = total_joblist.copy()
        if isinstance(temp,dict) and len(temp) > 1:
            write_vge_result1(temp, mpi_args["total_joblist_filename"]) 
            del temp
        write_vge_result2(worker_jobcount_list, worker_worktime_list,  mpi_args["vge_worker_result"])
        temp = command_list.copy()
        if isinstance(temp,dict) and len(temp) > 0:
            write_vge_result3(temp,  mpi_args["vge_commands"])


    return



