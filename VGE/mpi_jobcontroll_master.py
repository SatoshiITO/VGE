#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */

from mpi4py import MPI
import os,math
import multiprocessing
import select, socket, pickle
import time, datetime

from VGE.get_workerstatus import get_workerstatus
from VGE.get_target_worker import get_target_worker
from VGE.get_vge_conf import get_vge_conf
from VGE.send_emergencysignal import send_emergencysignal

def mpi_jobcontroll_master(cl_args, mpi_args, comm, total_joblist,new_joblist,task_check,pipeline_parent_pid_list,command_list):   

    #
    # MPI message tag
    #
    message_tag=10      # job message from the master
    forcedstop_tag=99 # forced stop message from the master

    from logging import getLogger,StreamHandler,basicConfig,DEBUG,INFO,WARNING,ERROR,CRITICAL
    logger=getLogger(__name__)
    logger.setLevel(INFO)

    #from guppy import hpy
    #hp=hpy()
    #hp.setrelheap()
    #logger.info("VGE(MPI): --------heap--------------- %s" %(hp.heap()))

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
    #number_of_jobs =len(total_joblist)
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
    #   worker_jobcount_list[ii] = 0
       worker_worktime_list[ii] = 0
       worker_wait_list[ii] = None
    for ii in range(1,nproc):
       worker_jobcount_list[ii] = 0
    #worker_jobcount_list[master_rank] = -1
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
    recv_check_list=[]

    # monitor running jobs on workers
    flag_monitor_worker_runjob=False
    if cl_args.monitor_workerjob:
       flag_monitor_worker_runjob=True
    #
    flag_emergencysignal=False
    check_worker_emergency_mesg=[-1]*nproc
    worker_emergency=[-1]*nproc

    # job schedule mode
    jobschedule_sample_list={}
    jobschedule_arrayjob_list={}
    flag_jobschedule_first=False
    flag_jobschedule_sample=False
    flag_jobschedule_arrayjob=False
    flag_jobschedule_mixmode=False
    #
    if cl_args.schedule == "first":
       flag_jobschedule_first=True
       logger.info("VGE(MPI): job schedule mode ... [first]")
    if cl_args.schedule == "sample":
       flag_jobschedule_sample=True
       logger.info("VGE(MPI): job schedule mode ... [sample]")
    if cl_args.schedule == "arrayjob":
       flag_jobschedule_arrayjob=True
       logger.info("VGE(MPI): job schedule mode ... [arrayjob]")
    if cl_args.schedule == "mix":
       flag_jobschedule_mixmode=True
       logger.info("VGE(MPI): job schedule mode ... [mix]")


    # ordered id for executed job
    execjobid=0
    #

    #
    #if (flag_jobschedule_first or flag_jobschedule_sample or flag_jobschedule_arrayjob \
    #      or flag_jobschedule_mixmode) is not True: 
    #   logger.error("VGE(MPI): ERRORR!! check .... %s" %cl_args.schedule)
    #   exit()
    #else: 
    logger.debug("VGE(MPI): flag_jobschedule_first...... %s" %flag_jobschedule_first)
    logger.debug("VGE(MPI): flag_jobschedule_sample..... %s" %flag_jobschedule_sample)
    logger.debug("VGE(MPI): flag_jobschedule_arrayjob... %s" %flag_jobschedule_arrayjob)

    # sampling mode
    flag_multisample=True
    #if cl_args.sample is "single":
    #   flag_multisample=False
    #   logger.info("VGE(MPI): Pipeline sample mode ... [single-runs]")
    #else:
    #   logger.info("VGE(MPI): Pipeline sample mode ... [multiple-runs]")

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
                flag = comm.iprobe(source=ii, tag=message_tag, status=None)
                if flag: 
                    worker_probe_list[ii] = (jj+1)
                    break
                else:
                    time.sleep(nsleep_probe)
                    pass
        time_probe_test=time.time() - time_probe_test
        #
        logger.info("VGE(MPI): .....done. [%0.3e sec]" %time_probe_test)
        logger.info("VGE(MPI): MPI probe test for %i workers --> probed 1:[%i] 20>:[%i] >=20:[%i] not probed:[%i] (max. trial:%i)" \
               %(nproc-1, worker_probe_list.count(1), \
               len([1 for ii in worker_probe_list if 1 < ii and ii <20]), \
               len([1 for ii in worker_probe_list if ii >=20 ]), \
               worker_probe_list.count(0), max_probe_trial))
        logger.debug("VGE(MPI): worker probe count list:")
        logger.debug("VGE(MPI): "+ ''.join('{}:{}|'.format(*k) for k in enumerate(worker_probe_list))) 

    #
    # for jobscheduler mode first only 
    #
    local_new_joblist = dict()
    icount_local_new_joblist = 0
    max_of_current_joblist = (nproc-1) + 5 # max of job pool size in current_joblist
    max_check_count=100
    task_check_restofjob=0
    #

    #//////////////////////////
    # job loop for the master node
    #//////////////////////////
    #loopcount = 0
    #loopcount_job = 0
    #total_icheck=0
    while still_have_jobs:
       #
       #logger.info("VGE(MPI): worker_jobcount_list (%s) " %worker_jobcount_list)
       #loopcount +=1

       if default_flag_loadbalancer is not flag_loadbalancer and command is not None: 
           #logger.debug("default_flag_loadbalancer =%s flag_loadbalancer=%s" %(default_flag_loadbalancer,flag_loadbalancer))
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
       # the master continues to check new jobs from pipeline job controller, 
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
       # target worker searching loop
       #
       search_target_worker=True
       #icheck=0
       while search_target_worker:
           #icheck +=1
           #total_icheck +=1
           #
           # check new jobs
           #
           if flag_jobschedule_first:
              if  icount_local_new_joblist == 0:
              #if len(local_new_joblist) ==0:
                  if len(new_joblist) != 0:  # got new jobs
                     local_new_joblist = new_joblist.copy()
                     icount_local_new_joblist = len(local_new_joblist) # update this value at this time
                     logger.info( "VGE(MPI): got new job(s) from VGE(JOB)")

              #if len(local_new_joblist) !=0:
              #if  icount_local_new_joblist != 0:
              #if completed_newjob_package is False or (completed_newjob_package is True and command == None):
              if max_of_current_joblist >=  task_check_restofjob:
                 if  icount_local_new_joblist > 0:
                    # get one new job if new job is not packaged yet.
                    (newjobid , newjob) = local_new_joblist.popitem()
                    icount_local_new_joblist -=1 # decrement this value
                    #logger.debug("VGE(MPI): new job contents: [%s][%s]"  %(newjobid, newjob))
                    if newjob.has_key("status") is True:
                       if newjob["status"] is not None: 
                          #jobid = number_of_jobs  # this is because the jobid starts from zero.
                          current_joblist[newjobid] = newjob
                          #total_joblist[newjobid] = newjob
                          #number_of_jobs +=1 # update maximum number of jobs so far
                          task_check_restofjob +=1
                          del new_joblist[newjobid]  # delete this order because we have already received
                          time.sleep(nsleep_updatelist)
                          #logger.debug("VGE(MPI): new job contents: [%s][%s]"  %(newjobid, newjob))
                    else:
                       pass # this dict.copy was incompleted.

                 #task_check_restofjob = len(current_joblist)
                 #logger.info("VGE(MPI): rest of jobs to be carried out is [%i] " %(task_check_restofjob))
                 #
              if command == None and completed_newjob_package:
                 completed_newjob_package=False

           else: # except for "first" schedule mode
              if len(new_joblist) != 0:  # got new jobs
                 logger.info( "VGE(MPI): got new job(s) from VGE(JOB)")
                 #if cl_args.debug:
                      #temp = new_joblist.copy()
                      #import pprint
                      #pp = pprint.PrettyPrinter(indent=4)
                      #logger.debug("VGE(MPI): new job contents: %s"  %pp.pprint(temp))
                      #del temp
                 #logger.debug("VGE(MPI): new job contents: %s"  %new_joblist)
                 for newjobid, newjob in new_joblist.items():
                      if newjob.has_key("status") is True:
                          if  newjob["status"] is not None: 
                              #jobid = number_of_jobs  # this is because the jobid starts from zero.
                              current_joblist[newjobid] = newjob
                              #total_joblist[newjobid] = newjob
                              #number_of_jobs +=1 # update maximum number of jobs so far
                              del new_joblist[newjobid]  # delete this order because we have already received
                              time.sleep(nsleep_updatelist)
                 #logger.debug("VGE(MPI): update total joblist: %s"  %total_joblist)
                 #logger.debug("VGE(MPI): update current_joblist: %s"  %current_joblist)
                 #
                 task_check_restofjob = len(current_joblist)
                 logger.info("VGE(MPI): rest of jobs to be carried out is [%i] " %(task_check_restofjob))
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
               #
               #
               jobid = None
               job = {}
               command = None 
               command_id = None
               filename = None
               flag_target_job=False
               bulkjob_id = None
               #
               #
               #if len(current_joblist) != 0:
               #task_check["restofjob"]=len(current_joblist)
               #logger.info("VGE(MPI): rest of jobs to be carried out is [%i] " %(task_check["restofjob"]))
               #
               if task_check_restofjob != 0:

                   # 
                   # preparation of list
                   # 

                   # sample list
                   
                   if flag_jobschedule_sample or flag_jobschedule_mixmode:
                      #
                      # update jobschedule_sample_list
                      #
                      num_of_pipeline_parent_pid_list = len(pipeline_parent_pid_list)
                      num_of_jobschedule_sample_list = len(jobschedule_sample_list)
                      logger.debug("VGE(MPI): check  num_of_pipeline_parent_pid_list[%i]  num_of_jobschedule_sample_list [%i]" \
                                     %(num_of_pipeline_parent_pid_list ,num_of_jobschedule_sample_list))
                      #
                      if num_of_pipeline_parent_pid_list == num_of_jobschedule_sample_list: # list was unchanged.
                         pass
                      elif num_of_jobschedule_sample_list < num_of_pipeline_parent_pid_list:
                          temp_pipeline_parent_pid_list_set=set(pipeline_parent_pid_list.keys()) # pipeline_parent_pid_list is dict.
                          temp_jobschedule_sample_list_set=set(jobschedule_sample_list.keys()) # jobschedule_sample_list is dict.
                          diff_pipeline_parent_pid_list = list(temp_pipeline_parent_pid_list_set - temp_jobschedule_sample_list_set)
                          for temp_parent_pid in diff_pipeline_parent_pid_list:
                              jobschedule_sample_list[int(temp_parent_pid)]=False  # if parent_pid is False, the job for this pid is not performed in this turn yet.
                      else:
                          logger.error("VGE(MPI): ERROR! something is wrong!!  num_of_pipeline_parent_pid_list[%i]  \
                                        num_of_jobschedule_sample_list [%i]" %(num_of_pipeline_parent_pid_list ,num_of_jobschedule_sample_list))

                      #
                      logger.debug("VGE(MPI): check pipeline_parent_pid_list ...[%s] " %pipeline_parent_pid_list)
                      logger.debug("VGE(MPI): check jobschedule_sample_list ...[%s] " %jobschedule_sample_list)
                      # 
                      # detele a pipeline sample from the jobschedule list if it finished.
                      #
                      if True in pipeline_parent_pid_list.values():
                         for pipeline_ppid, value in pipeline_parent_pid_list.items():
                             if value is True:
                                 #
                                 del jobschedule_sample_list[pipeline_ppid]
                                 #
                                 logger.debug("VGE(MPI): check... found finished Pipeline ppid ...[%s]" %pipeline_ppid)
                                 logger.debug("VGE(MPI): updated jobschedule_sample_list ...[%s] " %jobschedule_sample_list)
      
                      #
                      # get a target Pipeline parent (sample) pid
                      #
                      if False in jobschedule_sample_list.values():
                         pass # jobschedule_sample_list still has False values
                      else:
                         jobschedule_sample_list = dict.fromkeys(jobschedule_sample_list, False) # jobschedule_sample_list did not have False ,say all True, so initialize it.
                      #
                      target_parent_pid=None
                      #
                      sorted_jobschedule_sample_list= sorted(jobschedule_sample_list.items(), key=lambda x:x[1]) # sorted by False first  (a tuple)
                      logger.debug("VGE(MPI): check  sorted_jobschedule_sample_list....[%s] " %sorted_jobschedule_sample_list)
                    

                   # arrayjob list
                   if flag_jobschedule_arrayjob or flag_jobschedule_mixmode:
                      # 
                      # make a set of current arrayjob ids
                      #
                      current_arrayjob_id_set = set()
                      for jobid_temp, job_temp in current_joblist.items():
                         if job_temp["sendtoworker"] == False: 
                             current_arrayjob_id_set.add(int(job_temp["pipeline_pid"]))
                      #
                      # get the job for a target arrayjob id
                      #
                      if len(current_arrayjob_id_set) > 0:
                         #
                         logger.debug("VGE(MPI): check ...current_arrayjob_id_set [%s] " %current_arrayjob_id_set)
                         #
                         temp_dict = jobschedule_arrayjob_list.copy() # previous jobschedule_arrayjob_list
                         #
                         # update jobschedule_arrayjob_list
                         #
                         jobschedule_arrayjob_list = dict.fromkeys(current_arrayjob_id_set, False)
                         #
                         if True in temp_dict.values(): 
                             for temp_pid, temp_value in temp_dict.items():
                                if temp_value is True:
                                   if jobschedule_arrayjob_list.has_key(int(temp_pid)):
                                      jobschedule_arrayjob_list[temp_pid] = True
                         #
                         #
                         if False in jobschedule_arrayjob_list.values():
                            pass
                         else:
                            jobschedule_arrayjob_list = dict.fromkeys(jobschedule_arrayjob_list, False) 
                         #
                         sorted_jobschedule_arrayjob_list= sorted(jobschedule_arrayjob_list.items(), key=lambda x:x[1]) # sorted by False first
                         #
                         logger.debug("VGE(MPI): check ...jobschedule_arrayjob_list [%s]" %jobschedule_arrayjob_list)
                         logger.debug("VGE(MPI): check ...sorted_jobschedule_arrayjob_list [%s]" %sorted_jobschedule_arrayjob_list)
                         #
                         target_arrayjob_pid=None

                   #
                   # joblist-ordered schedule (first come, first served)
                   # 
                   if flag_jobschedule_first:
                      for jobid_temp, job_temp in current_joblist.iteritems():
                         if job_temp["sendtoworker"] == False: 
                            jobid= jobid_temp
                            job = job_temp.copy()
                            flag_target_job=True
                            #logger.debug("VGE(MPI): found ...[%s] [%s] " %(jobid,job))
                            break # escape from this loop.
                   #
                   # sample-ordered schedule 
                   #
                   elif flag_jobschedule_sample:
                      # 
                      for sample in sorted_jobschedule_sample_list:
                          #sample_pid = sample[0]
                          #sample_pid_values= sample[1]
                          target_sample_pid= int(sample[0]) # lock on!
                          #
                          logger.debug("VGE(MPI): check  target_sample_pid....[%i] " %target_sample_pid)
                          #
                          flag_found=False
                          #
                          for jobid_temp, job_temp in current_joblist.iteritems():
                             if int(job_temp["pipeline_parent_pid"]) == target_sample_pid:
                                if job_temp["sendtoworker"] == False: 
                                   #
                                   jobid = jobid_temp
                                   job = job_temp.copy()
                                   logger.debug("VGE(MPI): found the target pipeline parent pid ...[%i] " %target_sample_pid)
                                   #
                                   flag_target_job=True
                                   flag_found=True
                                   #
                                   jobschedule_sample_list[target_sample_pid]=True
                                   #
                                   break # exit this loop

                          #
                          if flag_found:
                             break # exit this loop
                   #
                   # arrayjob-ordered schedule 
                   #
                   elif flag_jobschedule_arrayjob:
                      #
                      for arrayjob in sorted_jobschedule_arrayjob_list:
                         # 
                         target_arrayjob_pid= int(arrayjob[0]) # this is a target arrayjob id in this turn.
                         arrayjob_pid_value = arrayjob[1]
                         #
                         flag_found=False
                         #
                         for jobid_temp, job_temp in current_joblist.iteritems():
                            if int(job_temp["pipeline_pid"]) == target_arrayjob_pid:
                               if job_temp["sendtoworker"] == False: 
                                  #
                                  jobid= jobid_temp
                                  job = job_temp.copy()
                                  #
                                  flag_found=True
                                  flag_target_job=True
                                  #
                                  jobschedule_arrayjob_list[target_arrayjob_pid]=True
                                  #
                                  logger.debug("VGE(MPI): [array] found1 ...target_arrayjob_pid[%i] jobid[%s] job[%s] " %(target_arrayjob_pid, jobid,job))
                                  break # escape from this loop.
                         #
                         if flag_found:
                            break # escape from this loop.
                   #
                   # a mix of sample and arrayjob ordered schedule
                   #
                   elif flag_jobschedule_mixmode:
                      #
                      for sample in sorted_jobschedule_sample_list:
                         #
                         target_sample_pid= int(sample[0]) # lock on!
                         #
                         flag_found1=False
                         #
                         for arrayjob in sorted_jobschedule_arrayjob_list:
                            # 
                            target_arrayjob_pid= int(arrayjob[0]) # lock on!
                            #
                            flag_found2=False
                            #
                            for jobid_temp, job_temp in current_joblist.iteritems():
                               if int(job_temp["pipeline_pid"]) == target_arrayjob_pid and int(job_temp["pipeline_parent_pid"]) == target_sample_pid:
                                  if job_temp["sendtoworker"] == False: 
                                     #
                                     jobid= jobid_temp
                                     job = job_temp.copy()
                                     #
                                     flag_found1=True
                                     flag_found2=True
                                     flag_target_job=True
                                     #
                                     jobschedule_sample_list[target_sample_pid]=True
                                     jobschedule_arrayjob_list[target_arrayjob_pid]=True
                                     #
                                     logger.debug("VGE(MPI): [array] found1 ...target_sample_pid [%i] target_arrayjob_pid[%i] jobid[%s] job[%s] " %(target_sample_pid,target_arrayjob_pid, jobid,job))
                                     break # escape from this loop.
                            #
                            if flag_found2:
                               break # escape from this loop.
                         #
                         if flag_found1:
                            break # escape from this loop.
                            
                   #
                   # 
                   #
                   if flag_target_job:
                      if job.has_key("filename") and job.has_key("command_id") and job.has_key("bulkjob_id"): 
                         #
                         filename= job["filename"]
                         command_id = job["command_id"]
                         bulkjob_id = job["bulkjob_id"]
                         #

                         # update current_joblist
                         current_joblist[jobid]["sendtoworker"] = True 

                         # replace VGE_BULKJOB_ID in the command with blukjob_id 
                         temp = ""
                         temp = command_list[command_id]["command"]
                         command = temp.replace("VGE_BULKJOB_ID", str(bulkjob_id))
                         #del temp

                         #
                         # update total_joblist
                         #
                         if total_joblist.has_key(jobid):
                            #
                            temp = total_joblist[jobid]
                            temp["sendtoworker"] = True
                            #
                            # increment execjobid
                            #
                            temp["execjobid"] = execjobid
                            execjobid +=1 
                            #
                            total_joblist[jobid]=temp
                            #
                            #del temp
                            #time.sleep(nsleep_updatelist)
                            #break # escape from this loop.

               #
               # package a job
               #
               package={}
               package["command"] = command
               package["filename"] = filename
               package["jobid"] = jobid
               package["bulkjob_id"] = bulkjob_id
               if command is not None:
                  completed_newjob_package=True
               #
               #
               #

           #if task_check_restofjob == 0:
           #if icheck > max_check_count:
           #   icheck = 0
           #   #
           #   # got an emergency message from VGE_CONNECT or Pipeline?
           #   #
           #   #if task_check.has_key("forced"):
           #   #   if task_check["forced"] is True: # 
           #   #       #logger.debug("VGE(MPI): got an emergency signal from VGE(JOB)! ")
           #   #       if flag_monitor_worker_runjob: 
           #   #          # send an emergency message to all workers. if worker is doing a job, then worker will kill the job.
           #   #          #logger.debug("VGE(MPI): check flag_monitor_worker_runjob") 
           #   #          if not flag_emergencysignal: 
           #   #             worker_emergency, check_worker_emergency_mesg=send_emergencysignal(comm, nproc, master_rank, forcedstop_tag)
           #   #             logger.info("VGE(MPI): emergency signal was send to all workers (p1)" )
           #   #             logger.debug("VGE(MPI): check ... worker_emergency,check_worker_emergency_mesg.... [%s] [%s] " %(check_worker_emergency_mesg,worker_emergency))
           #   #             flag_emergencysignal=True

           #
           # got a final singal from Pipeline?
           #
           if task_check_restofjob == 0:
             if task_check.has_key("exit"):
               if task_check["exit"]:
                   change_loadbalancer=True
                   search_target_worker=False
                   logger.info("VGE(MPI): got a termination signal from VGE(JOB)...")
                   if flag_loadbalancer: 
                      logger.info("VGE(MPI): change load balancer mode -> any worker")
                      flag_loadbalancer=False
                   #
                   break 

           #
           #
           #
           if flag_loadbalancer:
               #
               # get a target worker 
               #
               #task_check_restofjob=len(current_joblist)
               #if len(current_joblist) > 0:
               if task_check_restofjob > 0:
                   # check
                   target_worker_rank = -1
                   found_worker=False
                   jj = 0

                   #
                   if completed_newjob_package:
                       if None in worker_wait_list.values():
                           for rank_temp, check_flag in worker_wait_list.iteritems():
                               if check_flag is None:
                                   jj += 1
                                   search_target_worker=False
                                   target_worker_rank = rank_temp
                                   flag_found_target=True
                                   found_worker=True
                                   break #exit this loop
                   if found_worker:
                       check_worker="found target worker[%s]: 1st(None) -- jj(%i) sort(%0.3e)" %(target_worker_rank,jj,time_sort_list)
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
                           sorted_node_list = sorted(worker_jobcount_list.iteritems(), key = lambda x:x[1])
                           #logger.debug("node_joboucnt_list (%s)" %worker_jobcount_list)
                           #logger.debug("sorted_node_joboucnt_list (%s)" %sorted_node_list)
                       elif flag_worktime_loadbalancer:
                           sorted_node_list = sorted(worker_worktime_list.iteritems(), key = lambda x:x[1])
                           #logger.debug("worker_worktime_list (%s)" %worker_worktime_list)
                           #logger.debug("sorted_node_joboucnt_list (%s)" %sorted_node_list)
                       time_sort_list =time.time() - time_sort_list

                       flag_new_sendjob=False
                       for jobid_temp, job in current_joblist.iteritems():
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
                           check_worker="found target worker[%s]: 1st -- jj(%i) sort(%0.3e)" %(target_worker_rank,jj,time_sort_list)
                       else:
                           check_worker="found target worker[%s]: 2nd -- jj(%i) sort(%0.3e)" %(target_worker_rank,jj,time_sort_list)
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
               found_worker=False

               # 1st priority
               if completed_newjob_package:
                  #loopcount_job +=1
                  #if None in worker_wait_list.values():
                  #    for rank_temp, check_flag in worker_wait_list.iteritems():
                  #        if check_flag is None:
                  #            search_target_worker=False
                  #            target_worker_rank = rank_temp
                  #            flag_found_target=True
                  #            found_worker=True
                  #            logger.debug("VGE(MPI): found target worker[%s]: None / any worker" %(target_worker_rank))
                  #            break #exit this loop
                  #else : 
                  #    search_target_worker=False
                  #    target_worker_rank = None
                  #    flag_found_target=False
                  #    found_worker=False
                  #    break #exit this while loop

                  # if the master has a new job to be send to workers, go MPI_rev point right now !
                  search_target_worker=False
                  target_worker_rank = None
                  flag_found_target=False
                  found_worker=False
                  break #exit this while loop

               else: # completed_newjob_package is False ( the master does not have a new job)
                  #
                  # 2nd priority
                  #
                  flag_new_sendjob=False
                  if task_check_restofjob >0:
                      #for jobid_temp, job in current_joblist.iteritems():
                      for job in current_joblist.itervalues():
                          if job["sendtoworker"] == False or (job["sendtoworker"] == True and job["status"] == "ready"):
                              # job package is not completed but we have a job to be send.
                              flag_new_sendjob=True
                              break #exit this loop then go to while-loop top
                  #
                  # 3rd priority
                  #
                  if flag_new_sendjob is False:
                     for job in current_joblist.itervalues():
                        if job["sendtoworker"] == True and job["status"] == "wait":
                           target_worker_rank = int(job["worker"])
                           flag_probe = get_workerstatus(comm, target_worker_rank, number_of_probe, nsleep_probe)
                           if flag_probe:
                               flag_found_target=True
                               #logger.debug("VGE(MPI) ... get job status from worker[%i]" %target_worker_rank)
                               search_target_worker=False
                               break  #exit this loop
                     if flag_found_target:
                        break #exit this while loop

                     # check a signal from a worker
                     flag_probe = get_workerstatus(comm, MPI.ANY_SOURCE, number_of_probe, nsleep_probe)
                     if flag_probe:
                        search_target_worker=False
                        break #exit this while loop
                        #

                           

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
       #if flag_loadbalancer or flag_found_target:
       #    #worker_jobstatus = comm.recv(source=target_worker_rank, tag=MPI.ANY_TAG, status=status)
       #    worker_jobstatus = comm.recv(source=target_worker_rank, tag=message_tag, status=status)
       #    flag_found_target=False
       #else:
       #    #worker_jobstatus = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
       #    worker_jobstatus = comm.recv(source=MPI.ANY_SOURCE, tag=message_tag, status=status)
       ##if flag_loadbalancer or flag_found_target:
       ##    #worker_jobstatus = comm.recv(source=target_worker_rank, tag=MPI.ANY_TAG, status=status)
       ##    req = comm.irecv(worker_jobstatus, source=target_worker_rank, tag=message_tag)
       ##    flag_found_target=False
       ##else:
       ##    #worker_jobstatus = comm.recv(source=MPI.ANY_SOURCE, tag=MPI.ANY_TAG, status=status)
       ##    req = comm.irecv(worker_jobstatus, source=MPI.ANY_SOURCE, tag=message_tag)

       if_flag_recve = True
       flag_probe = False
       while if_flag_recve:
           if flag_loadbalancer or flag_found_target:
               flag_probe = get_workerstatus(comm, target_worker_rank, number_of_probe, nsleep_probe)
           else:
               flag_probe = get_workerstatus(comm, MPI.ANY_SOURCE, number_of_probe, nsleep_probe)

           if task_check.has_key("forced"):
               if task_check["forced"] is True: 
                    command = None # repackaged as an empty job if this package has a job. 
                    package["command"] = command
                    if flag_monitor_worker_runjob: 
                       if not flag_emergencysignal:
                          worker_emergency, check_worker_emergency_mesg=send_emergencysignal(comm, nproc, master_rank, forcedstop_tag)
                          logger.info("VGE(MPI): emergency signal was send to all workers (p1)" )
                          flag_emergencysignal=True
           
           if flag_probe:
               if flag_loadbalancer or flag_found_target:
                   worker_jobstatus = comm.recv(source=target_worker_rank, tag=message_tag, status=status)
                   flag_found_target=False
               else:
                   worker_jobstatus = comm.recv(source=MPI.ANY_SOURCE, tag=message_tag, status=status)
               break
  
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
            #logger.info("VGE(MPI): --------heap-------------- %s" %(hp.heap()))
            pass
       if command is None and jobid is None:
            jobid = worker_source  # if command is empty so jobid will be assigned to the worker rank to avoid an error
       #
       if task_check.has_key("forced"):
           if task_check["forced"] is True: 
                command = None # repackaged as an empty job if this package has a job. 
                package["command"] = command
                if flag_monitor_worker_runjob: 
                   if not flag_emergencysignal:
                      worker_emergency, check_worker_emergency_mesg=send_emergencysignal(comm, nproc, master_rank, forcedstop_tag)
                      logger.info("VGE(MPI): emergency signal was send to all workers (p2)" )
                      #logger.debug("VGE(MPI): check ... worker_emergency,check_worker_emergency_mesg.... [%s] [%s] " %(check_worker_emergency_mesg,worker_emergency))
                      flag_emergencysignal=True

       start_send=time.time()
       #logger.debug("master:check before send")
       #comm.send(package, dest=worker_source, tag=jobid)
       comm.send(package, dest=worker_source, tag=message_tag)
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
                   #
                   if worker_jobstatus["status"] == "done":
                      # delete finished job keys in current_joblist
                      del current_joblist[worker_jobstatus["jobid"]]
                      task_check_restofjob -= 1 # decrement this value
                   else:
                      # update current_joblist
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
                   #time.sleep(nsleep_updatelist)

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
                   pass
                   #logger.debug("worker_source(%i) recv_list: %s" %(worker_source,recv_list))
                   #logger.debug("probe_time: %0.3e sec" %probe_time)
               #logger.debug("check_worker: %s" %check_worker)
               #logger.debug("flag_new_sendjob:   %s" %flag_new_sendjob)
           #logger.debug("worker_wait_list:   %s" %worker_wait_list)
           #logger.debug("worker_jobcount_list: %s" %worker_jobcount_list)
           #logger.debug("worker_worktime_list: %s" %worker_worktime_list)
       else:
           worker_wait_list[worker_source] = False

       if default_flag_loadbalancer: 
            flag_loadbalancer=True

       #
       # change the job status to "wait" and mark the worker rank for the job that be send.
       #
       #if len(current_joblist) != 0:
       #task_check["restofjob"]=len(current_joblist)
       if task_check_restofjob != 0:
           if command is not None and jobid is not None:
                #
                # update current_joblist
                #
                current_joblist[jobid]["status"] = "wait"
                current_joblist[jobid]["worker"] = worker_source # this is a worker rank number.
                #
                # update total_joblist
                #
                temp = total_joblist[jobid]
                temp["status"] = "wait"
                temp["worker"] = worker_source
                total_joblist[jobid]=temp
                time.sleep(nsleep_updatelist)
                #del temp

                #
       #logger.debug("VGE(MPI): %s"  %current_joblist)
                       
       #
       # delete finished job keys in current_joblist
       #
       #for jobid_temp, job in current_joblist.items():
       #    if job["status"] == "done" and job["sendtoworker"] == True:
       #         del current_joblist[jobid_temp]
       #         task_check_restofjob -= 1:

       #
       # whether all jobs are done or not
       #
       if command is None :
          if task_check_restofjob == 0:
            if task_check.has_key("exit"):
              if task_check["exit"]==True:
                  Final_flag = True
                  for jobid_temp, job in total_joblist.items():
                     if job["status"] !=  "done":
                         Final_flag = False  # still have jobs...
                         break
                  #
                  if Final_flag == True: 
                     still_have_jobs=False # all the jobs to be performed were done!
                     logger.info( "VGE(MPI): VGE MPI master controller finished normally.")
       else:
       #
          #task_check["restofjob"]=len(current_joblist)
          logger.info("VGE(MPI): rest of jobs to be carried out is [%i] " %(task_check_restofjob))
          time_job = time.time() - start_job
          if time_sort_list > 0.0 :
              logger.info("VGE(MPI): job [id:%d] dispatch -> worker [%i]: communication elapsed time -> mesg-recv[%0.3e]/send[%0.3e] sort[%0.3e] job-dispatch[%0.3e] 1-cycle[%0.3e] sec" \
                       %(jobid, worker_source, end_recv-start_recv, end_send-start_send, time_sort_list, dispatch_time, time_job))
          else:
              logger.info("VGE(MPI): job [id:%d] dispatch -> worker [%i]: communication elapsed time -> mesg-recv[%0.3e]/send[%0.3e] job-dispatch[%0.3e] 1-cycle[%0.3e] sec" \
                       %(jobid, worker_source, end_recv-start_recv, end_send-start_send, dispatch_time, time_job))
          
       #
       # forced to shutdown ?
       #
       if task_check.has_key("forced"): 
          if task_check["forced"] is True: # 
             if still_have_jobs is False:
                logger.info("VGE(MPI): already going to shutdown normally although the forced-stop signal recieved.")
             else:
                still_have_jobs=False # turn this switch off !
                logger.info("VGE(MPI): VGE MPI master controller aborted because got a forced-stop signal.")
                #
                #logger.debug("VGE(MPI): current_joblist: %s"  %current_joblist)
                #logger.debug("VGE(MPI): total_joblist %s"  %total_joblist)
             #
             #
             if flag_monitor_worker_runjob:
                if not flag_emergencysignal: 
                   # send an emergency message to all workers. if worker is doing a job, then worker will kill the job.
                   worker_emergency,check_worker_emergency_mesg=send_emergencysignal(comm, nproc, master_rank, forcedstop_tag)
                   logger.info("VGE(MPI): emergency signal was send to all workers (p3)" )
                   #logger.debug("VGE(MPI): check ... worker_emergency,check_worker_emergency_mesg.... [%s] [%s] " %(check_worker_emergency_mesg,worker_emergency))
                   flag_emergencysignal=True

    #
    # finalize
    #
    pass
    #
    #
    logger.info("VGE(MPI): finalize...")
    task_check_restofjob=len(current_joblist)
    if task_check_restofjob == 0:
       logger.info("VGE(MPI): all the jobs to be carried out were done [%i] " %(task_check_restofjob))
    else:
       logger.info("VGE(MPI): some of jobs were not performed.  [%i] " %(task_check_restofjob))


    #
    # write worker job status lists in json format
    #
    #import json
    #logger.debug("\nVGE(MPI): worker results [job count]:")
    #logger.debug(json.dumps(worker_jobcount_list,indent=1))
    #logger.debug("\nVGE(MPI): worker results [work time]:")
    #logger.debug(json.dumps(worker_worktime_list,indent=1))
    #logger.debug("VGE(MPI): VGE MPI controller finished normally.\n")

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
            del temp
    #
    #
    #if flag_emergencysignal is True:
    #   finalize_isend = None
    #   for worker_rank in range(nproc):
    #       if worker_rank is not master_rank :
    #           finalize_isend = worker_emergency[worker_rank].wait()
    #           logger.debug("VGE(MPI): flag: operation isend to worker [%i] was complete. [%s]" %(worker_rank, finalize_isend))
    #   del(finalize_isend)

    #
    # final communication to all workers
    #
    command = "terminate_all_workers" # 
    for i in range(nproc-1):
       #
       #
       worker_jobstatus= comm.recv(source=MPI.ANY_SOURCE, tag=message_tag, status=status)
       #
       #
       worker_source = status.Get_source()
       worker_tag = status.Get_tag()
       logger.debug("VGE(MPI): got a final message [%s] from worker [%i] " %(worker_jobstatus, worker_source))
       logger.info("VGE(MPI): from master [%i]: shutdown MPI worker [%i] " %(myrank, worker_source))
       #

       #
       comm.send(command, dest=worker_source, tag=message_tag)
       #
       #
       # update total_joblist if the message has a job status 
       #
       if isinstance(worker_jobstatus,dict):
         if worker_jobstatus.has_key("jobid"):
           if worker_jobstatus["jobid"] is not None: 
            #
            if total_joblist.has_key(worker_jobstatus["jobid"]):
               temp = total_joblist[worker_jobstatus["jobid"]]
               if temp.has_key("status"):
                   #logger.debug("VGE(MPI): check ...update total_joblist ")
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
    # 
    #
      
    #
    #
    #
    #logger.debug("VGE(MPI): flag: load balancer ---------> %s" %flag_loadbalancer)
    #logger.debug("VGE(MPI): flag: load balancer [worktime]:%s" %flag_worktime_loadbalancer)
    #logger.debug("VGE(MPI): flag: load balancer [jobcount]:%s" %flag_jobcount_loadbalancer)

    ##
    ## write worker job status lists in json format
    ##
    #import json
    #logger.debug("\nVGE(MPI): worker results [job count]:")
    #logger.debug(json.dumps(worker_jobcount_list,indent=1))
    #logger.debug("\nVGE(MPI): worker results [work time]:")
    #logger.debug(json.dumps(worker_worktime_list,indent=1))
    #logger.debug("VGE(MPI): VGE MPI controller finished normally.\n")
    #
    ##
    ## write csv formatted file.
    ##
    #if not cl_args.nowrite_vgecsv:
    #    from VGE.write_vge_result1 import write_vge_result1
    #    from VGE.write_vge_result2 import write_vge_result2
    #    from VGE.write_vge_result3 import write_vge_result3
    #    temp = total_joblist.copy()
    #    if isinstance(temp,dict) and len(temp) > 1:
    #        write_vge_result1(temp, mpi_args["total_joblist_filename"]) 
    #        del temp
    #    write_vge_result2(worker_jobcount_list, worker_worktime_list,  mpi_args["vge_worker_result"])
    #    temp = command_list.copy()
    #    if isinstance(temp,dict) and len(temp) > 0:
    #        write_vge_result3(temp,  mpi_args["vge_commands"])
    #        del temp



    #
    # VGE mpi-master finished.
    #
    return
    #
    #
    #



