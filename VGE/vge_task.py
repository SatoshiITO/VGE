#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */

import socket, pickle
import time, datetime
import subprocess, multiprocessing
import random, os ,sys
import argparse
#
from VGE.make_pipeline_jobstatuslist import make_pipeline_jobstatuslist
from VGE.get_vge_conf import get_vge_conf
from VGE.get_process_name import get_process_name
from VGE.get_pipeline_process_name_list import get_pipeline_process_name_list
#

def vge_task(arg1,arg2,arg3,arg4,arg5=None):
  # arg1: command to be performed
  # arg2: max_task
  # arg3: basefilename
  # arg4: parameter dict
  #   involved keys:               arguments
  #      verbose                   --> verbose
  #      host                      --> host
  #      port                      --> port
  #      bufsize                   --> bufsize
  #      nsleep_request            --> socket_interval_after
  #      nsleep_interval=0.5       --> socket_interval_request
  #      nsleep_connection_refused --> socket_interval_error
  #      nsleep_aftersend          --> socket_interval_send
  #      nsleep_updatelist         --> socket_interval_update
  #      nsleep_controller=0.01    --> socket_interval_close
  #      timeout_client=600.0      --> socket_timeout1
  #      timeout_ask=600.0         --> socket_timeout2
  #
  # arg5: for future use
  #       "multi" --> multi-sample mode
  #       Pipeline sample or process will be considered in vge_task.
  #       this needs more CPU resouces than single-sample mode.
  #       "single" or None --> single-sample mode
  #      
  #           
  total_time = time.time()
  # 

  #
  # return value
  #
  # it will be the lastest return code of jobs for new job 
  # and be a response message from VGE for the others.
  result = None

  #
  # socket
  #
  host = socket.gethostbyname(socket.gethostname())
  port = 8000
  bufsize = 16384

  #
  # get current process id
  #  
  process_id = os.getpid()
  #

  #
  # assign a header name 
  # 
  temp_processname = get_process_name(process_id)
  temp_processnamelist= temp_processname.split(" ")
  #
  pname="VGETASK(Client)" 
  #
  for temp0 in temp_processnamelist:
      temp1=os.path.basename(temp0)
      if "pipeline" in temp1:
         pname="VGETASK(Pipeline)"
         break
      elif temp1 == "vge_connect" :
         pname="VGETASK(CONNECT)"
         break
      elif temp1 == "vge" :
         pname="VGETASK(VGE)"
         break
  #
  del temp_processname
  del temp_processnamelist
  #
  #pname="VGETASK(Client)"
  #if "pipeline" in temp_processname: 
  #  pname="VGETASK(Pipeline)"
  #elif "vge_connect" in temp_processnamelist:
  #  pname="VGETASK(CONNECT)"
  #elif "vge" in temp_processnamelist and not "vge_connect" in temp_processnamelist:
  #  pname="VGETASK(VGE)"
  #print temp_processname, temp_processnamelist, pname
  #

  #
  # wait time 
  #
  nsleep_connection_refused=0.0
  nsleep_aftersend =0.00 # pause after send
  nsleep_updatelist=0.00 # 
  nsleep_request=20.0     # pause after asked job status to VGE
  nsleep_interval=20.0    # pause after job has been submitted.
  nsleep_controller=1.00 # wait time after connection timed out
  timeout_client=600.0
  timeout_ask=600.0

  # 
  # Config
  # 
  time_config = time.time()
  current_dir=os.getcwd()
  port = int(get_vge_conf("socket","port",port))
  bufsize = int(get_vge_conf("socket","bufsize",bufsize))
  nsleep_interval = get_vge_conf("pipeline","socket_interval_after",nsleep_interval)
  nsleep_request = get_vge_conf("pipeline","socket_interval_request", nsleep_request)
  nsleep_connection_refused = get_vge_conf("pipeline","socket_interval_error", nsleep_connection_refused)
  nsleep_aftersend = get_vge_conf("pipeline","socket_interval_send", nsleep_aftersend)
  nsleep_updatelist = get_vge_conf("pipeline","socket_interval_update",nsleep_updatelist)
  nsleep_controller = get_vge_conf("pipeline","socket_interval_close",nsleep_controller)
  timeout_client = get_vge_conf("pipeline","socket_timeout1", timeout_client)
  timeout_ask = get_vge_conf("pipeline","socket_timeout2", timeout_ask)
  time_config = time.time() - time_config
  # 
  if isinstance(arg4, argparse.Namespace):
     if arg4.socket_interval_after > 0:
         nsleep_request =  arg4.socket_interval_after
     if arg4.socket_interval_request > 0:
         nsleep_interval =  arg4.socket_interval_request
     if arg4.socket_interval_error > 0:
         nsleep_connection_refused = arg4.socket_interval_error
     if arg4.socket_interval_send > 0:
         nsleep_aftersend = arg4.socket_interval_send
     if arg4.socket_interval_update > 0:
         nsleep_updatelist = arg4.socket_interval_update
     if arg4.socket_interval_close > 0:
         nsleep_controller = arg4.socket_interval_close
     if arg4.socket_timeout1 > 0:
         timeout_client = arg4.socket_timeout1
     if arg4.socket_timeout2 > 0:
         timeout_ask = arg4.socket_timeout2


  #
  # verbose level
  # 
  verbose=0
  verbose = int(get_vge_conf("pipeline","verbose",verbose))
  from logging import getLogger,StreamHandler,basicConfig,DEBUG,INFO,WARNING,ERROR,CRITICAL
  logger=getLogger(__name__)
  if verbose == 0:
     logger.setLevel(CRITICAL)
  elif verbose == 1:
     logger.setLevel(INFO)
  elif verbose == 3:
     logger.setLevel(DEBUG)
  basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')

  logger.debug("%s pid[%i]: socket host=%s" %(pname,process_id,host))
  logger.debug("%s pid[%i]: socket port=%i" %(pname,process_id,port))
  logger.debug("%s pid[%i]: verbose level=%i" %(pname,process_id,verbose))
  logger.debug("%s pid[%i]: socket buffer size=%i" %(pname,process_id,bufsize))
  logger.debug("%s pid[%i]: socket timeout1 (job sending)=%12.5e" %(pname,process_id,timeout_client))
  logger.debug("%s pid[%i]: socket timeout2 (job waiting)=%12.5e" %(pname,process_id,timeout_ask))
  logger.debug("%s pid[%i]: socket_interval_after=%12.5e" %(pname,process_id, nsleep_interval))
  logger.debug("%s pid[%i]: socket_interval_send=%12.5e" %(pname,process_id,nsleep_aftersend))
  logger.debug("%s pid[%i]: socket_interval_request=%12.5e" %(pname,process_id, nsleep_request))
  logger.debug("%s pid[%i]: socket_interval_error=%12.5e" %(pname,process_id,nsleep_connection_refused))
  logger.debug("%s pid[%i]: socket_interval_update=%12.5e" %(pname,process_id, nsleep_updatelist))
  logger.debug("%s pid[%i]: socket_interval_close=%12.5e" %(pname,process_id, nsleep_controller))
  logger.debug("%s pid[%i]: config: elapsed time=%12.5e" %(pname,process_id, time_config))
  #

  # 
  # set a sampling mode
  #
  multi_sampling=True
  if arg5 is "single" :
     multi_sampling=False
     #logger.debug( "Pipeline pid[%i]: Pipeline sampling mode .... single" %(process_id))
  elif arg5 is "multi":
     multi_sampling=True
     #logger.debug( "Pipeline pid[%i]: Pipeline sampling mode .... multiple" %(process_id))
  elif arg5 is "response":
     multi_sampling=True
     logger.debug( "%s pid[%i]: Pipeline sampling mode .... none (response)" %(pname,process_id))

  #
  # get a pipeline_parent_pid for multi-sampling or response mode
  #
  pipeline_parent_pid=-1
  if multi_sampling:
     temp_pipeline_parent_pid=os.getppid()
     logger.debug( "%s pid[%i]: parent pid from os.getppid [%i]" %(pname,process_id,temp_pipeline_parent_pid))
     try: 
       #
       from VGE.pipeline_parent_pid import pipeline_parent_pid
       if temp_pipeline_parent_pid == pipeline_parent_pid:
          logger.debug("%s pid[%i]: VGE.pipeline_parent_pid [%i] is the same as that [%i] of os.getppid() in this process." %(pname,process_id,pipeline_parent_pid,temp_pipeline_parent_pid))
          if process_id == pipeline_parent_pid:
             logger.info("%s pid[%i]: this is a parent process [id %i]" %(pname,process_id, pipeline_parent_pid))
             pass
          elif pipeline_parent_pid == -1:
             # parent pid info was not used.
             pass
          elif process_id != pipeline_parent_pid:
             logger.info( "%s pid[%i]: parent process id [%i] (current process is a child)" %(pname,process_id, pipeline_parent_pid))
       else:
           logger.debug( "%s pid[%i]: VGE.pipeline_parent_pid [%i] is not the same as that [%i] of os.getppid() in this process." %(pname,process_id,pipeline_parent_pid,temp_pipeline_parent_pid))
           logger.info( "%s pid[%i]: current process [%i] is a main (parent) " %(pname,process_id,pipeline_parent_pid))
     except: 
         logger.debug( "%s pid[%i]: VGE.pipeline_parent_pid module was not be loaded beforehand." %(pname,process_id))
         #logger.debug( "Pipeline pid[%i]: parent pid has been taken from os.getppid() in the current process." %(process_id))
         logger.info( "%s pid[%i]: try to get a Pipeline parent id ...." %(pname,process_id))
         logger.info( "%s pid[%i]: get a parent process id from os.getppid()..... [%i] " %(pname,process_id,temp_pipeline_parent_pid))
         logger.info( "%s pid[%i]: search Pipeline related process in the master rank..." %(pname,process_id))
         #
         parent_process_name = ""
         current_process_name = ""
         parent_process_name = get_process_name(temp_pipeline_parent_pid)
         current_process_name = get_process_name(process_id)
         #
         logger.debug( "%s pid[%i]: check os.getppid() ..... [%i] " %(pname,process_id,temp_pipeline_parent_pid))
         logger.debug( "%s pid[%i]: check.... process_name from os.getppid() [%s] " %(pname,process_id,parent_process_name))
         logger.debug( "%s pid[%i]: check.... process_name from os.getpid() [%s] " %(pname,process_id,current_process_name))
         #
         pipeline_process_list=[]
         pipeline_process_list=get_pipeline_process_name_list("master")
         logger.info("%s pid[%i]: check .... pipeline_process_list [%s]." %(pname,process_id,pipeline_process_list))
         # 
         flag_parent=False
         flag_current=False
         for pipeline_process_name in pipeline_process_list:
            #logger.info( "Pipeline pid[%i]: check ...parent_process_name[%s], pipeline_process_name[%s]" %(process_id,parent_process_name,pipeline_process_name))
            if pipeline_process_name in parent_process_name:
               logger.debug("%s pid[%i]: found Pipeline-related name [%s] in a parent process [%s]." %(pname,process_id,pipeline_process_name,parent_process_name))
               flag_parent=True
               break
         for pipeline_process_name in pipeline_process_list:
             #logger.info( "Pipeline pid[%i]: check ...currnet_process_name[%s], pipeline_process_name[%s]" %(process_id,current_process_name,pipeline_process_name))
             if pipeline_process_name in current_process_name:
                 logger.debug("%s pid[%i]: found Pipeline-related name [%s] in a current process [%s]." %(pname,process_id,pipeline_process_name,current_process_name))
                 flag_current=True
                 break
         if flag_parent and flag_current:
             logger.info( "%s pid[%i]: current process is a child" %(pname,process_id))
             logger.info( "%s pid[%i]: parent Pipeline process id is set to be [%i] from os.getppid()." %(pname,process_id,temp_pipeline_parent_pid))
             pipeline_parent_pid = temp_pipeline_parent_pid 
         elif not flag_parent and flag_current:
             logger.info( "%s pid[%i]: this is on a main process." %(pname,process_id))
             logger.info( "%s pid[%i]: Pipeline parent process id is set to be [%i] of the current pid." %(pname,process_id,process_id))
             pipeline_parent_pid = process_id
         else:
             logger.info( "%s pid[%i]: this state should be unknown." %(pname,process_id))
             logger.info( "%s pid[%i]: Pipeline process id will not be considered." %(pname,process_id))
             pipeline_parent_pid = -1

  #
  # initialize parameters
  # 
  jobid =0
  messeage=""
  flag_pipeline_task_loop=True
  finished="False"
  current_unique_jobid=0
  current_vge_jobid = 0
  current_max_task = 0
  joblist = dict()
  #
  current_jobid=None
  Flag=True
  retry=1
  #
  current_unique_jobid = -1
  current_vge_jobid = -1
  current_max_task = -1
  #
  time_jobsend = 0.0
  time_jobrecv = 0.0
  time_waitsend = 0.0
  time_waitrecv = 0.0
  time_jobdispatch=0.0
  wait_time = 0.0

  #
  # create a new job 
  #
  joblist[jobid]= dict()
  joblist[jobid]=make_pipeline_jobstatuslist(0)
  joblist[jobid]["pipeline_pid"] = process_id
  joblist[jobid]["pipeline_parent_pid"] = pipeline_parent_pid
 
  #
  # args
  #
  # get command
  command = ""
  if isinstance(arg1,str):
     if arg1 != "":
         joblist[jobid]["command"] = arg1

  # get max_task
  max_task=0
  if not isinstance(arg2, bool) and isinstance(arg2, int):
     if arg2 > -1:
         joblist[jobid]["max_task"] = int(arg2) 

  # get basefilename
  if isinstance(arg3,str):
     if arg3 != "":
         joblist[jobid]["basefilename"] = arg3


  #
  # check a request...
  #
  flag_newjob=True
  #
  # 

  #
  # shutdown VGE
  #
  if arg1 == "finished":
      logger.info("%s pid[%i]: shutdown VGE..." %(pname,process_id))
      joblist = "finished" 
      flag_newjob=False
      joblist_shipped = pickle.dumps(joblist)
  #
  # shutdown VGE forcibly
  #
  elif arg1 == "forcedstop":
      logger.info("%s pid[%i]: forced to shutdown VGE..." %(pname,process_id))
      joblist = "forcedstop"
      flag_newjob=False
      joblist_shipped = pickle.dumps(joblist)
  #
  # wake VGE up
  #
  elif arg1 == "hello":
      logger.info("%s pid[%i]: start up VGE..." %(pname,process_id))
      joblist = "hello" 
      flag_newjob=False
  #
  # request a list of Pipeline paretent process id which VGE knows.
  #
  elif arg1 == "pipeline_ppid_list":
      logger.info("%s pid[%i]: request Pipeline parent process id list..." %(pname,process_id))
      joblist = "pipeline_ppid_list"
      flag_newjob=False

  #
  # request the rest of jobs to be performed by VGE
  #
  elif arg1 == "restofjob":
      logger.info("%s pid[%i]: request the number of queued jobs which VGE has..." %(pname,process_id))
      joblist = "restofjob"
      flag_newjob=False

  #
  # send a wakeup messeage to VGE.
  #
  elif arg1 == "hello_from_pipeline":
      logger.info("%s pid[%i]: send hello from a Pipeline to VGE..." %(pname,process_id))
      joblist = dict()
      joblist["hello_from_pipeline"] = True
      joblist["pipeline_parent_pid"] = pipeline_parent_pid
      flag_newjob=False
  #
  # mark a pipeline byebye flag bacause a Pipeline was finished.
  #
  elif arg1 == "byebye_from_pipeline":
      logger.info("%s pid[%i]: send byebye from a Pipeline to VGE..." %(pname,process_id))
      joblist = dict()
      joblist["byebye_from_pipeline"] = True
      joblist["pipeline_parent_pid"] = pipeline_parent_pid
      flag_newjob=False

  #
  # package a joblist
  #
  if not flag_newjob:
      joblist_shipped = pickle.dumps(joblist)

  #
  # send the job to VGE
  #
  time_connection = time.time()
  while Flag:
          try:
                time_jobdispatch=time.time()
                try:
                   sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                except Exception,error: 
                   logger.info("%s pid[%i]: socket error [%s]." %(pname,process_id,error))

                if arg1 == "finished" or retry > 1:
                    sock.settimeout(timeout_client)
                #
                #
                mesg = ""
                messeage=""
                #
                # connect with VGE
                #
                if retry==1:
                    pass
                    #logger.info("Pipeline pid(%i): try to connect with VGE." %process_id)
                #
                sock.connect((host, port))
                logger.info("%s pid[%i]: connection with VGE was established. [access times:%i] " %(pname,process_id,retry))
                time_connection = time.time() -time_connection

                if isinstance(joblist, dict) and flag_newjob:
                    joblist_shipped=""
                    joblist[jobid]["sendvgetime"] = datetime.datetime.now()
                    # delete keys which are not used in VGE
                    temp=joblist[jobid].copy()
                    del temp["vge_jobid"]
                    #
                    joblist_shipped = pickle.dumps(temp)

                #
                # send the messeage(order) to VGE
                #
                #logger.info("Pipeline pid[%i]: new job has been submitted to VGE. [%s]" %(process_id, pickle.loads(joblist_shipped)))
                try: 
                    time_jobsend = time.time()
                    #logger.info("check...")
                    #logger.info(joblist_shipped)
                    sock.sendall(joblist_shipped)
                    time_jobsend = time.time() - time_jobsend
                    time.sleep(nsleep_aftersend)
                except socket.error:
                    logger.critical("%s   Error!! socket sending was failed. [data size->(%i) byte :(%s)]" %(pname,len(joblist_shipped), pickle.loads(joblist_shipped)))
                    pass
                    #sys.exit()
                except Exception,error:
                    logger.debug("%s pid[%i]: data send error (%s)"  %(pname,process_id, error))
                    pass
                #
                # receive an incoming messeage(the jobid for this order) from VGE
                #
                #mesg = sock.recv(bufsize)
                temp=None
                mesg=""
                icount=0
                time_jobrecv = time.time()
                while True:
                    temp = sock.recv(bufsize)
                    icount += 1
                    mesg += temp
                    if temp == "" or temp is None:
                        break
                    elif temp[-2:] == "\n." or temp[-3:] == "\ns.":
                        break
                    elif temp[-1:] == ".":
                        if mesg[-2:] == "\n." or mesg[-3:] == "\ns." :
                          break
                time_jobrecv = time.time() - time_jobrecv

                if isinstance(mesg,str):
                    messeage=None
                    if mesg > 0:
                        logger.info("%s pid[%i]: recieved mesg size=%i [%i recv times]" %(pname,process_id,len(mesg),icount))
                        logger.debug("%s pid[%i]: recieved mesg %s]" %(pname,process_id,mesg))
                    try:
                        messeage=pickle.loads(mesg)
                        logger.info("%s pid[%i]: response from VGE: %s"  %(pname,process_id, messeage))
                    except EOFError:
                        pass
                    except ValueError:
                        logger.error("%s pid[%i]: Value error. mesg is not recieved correctly. [bufsize:%i] [mesg:%s]" %(pname,process_id,bufsize,mesg))
                    except Exception,error:
                        logger.error("%s pid[%i]: exception error.... check [%s]" %(pname,process_id,error))

                    #try: 
                    #      messeage=pickle.loads(mesg)
                    #      logger.info("Pipeline pid[%i]: response from VGE: %s"  %(process_id, messeage))
                    #except EOFError: 
                    #      pass
                    #

                    #
                    # package a message from VGE as a return value if not flag_newjob.
                    #
                    if not flag_newjob: 
                       if isinstance(messeage, dict) and joblist is "pipeline_ppid_list":
                          result=messeage
                       elif isinstance(messeage, str) and joblist is "restofjob":
                          result=messeage

                    #
                    # check the response 
                    #
                    if isinstance(messeage, dict) and flag_newjob:
                       current_unique_jobid = int(messeage["unique_jobid"])
                       current_vge_jobid = int(messeage["vge_jobid"])
                       current_max_task = int(joblist[jobid]["max_task"])
                       check_pipeline_pid = int(messeage["pipeline_pid"])
                       joblist[jobid]["unique_jobid"] = current_unique_jobid
                       joblist[jobid]["vge_jobid"] = current_vge_jobid
                       #
                       logger.info("%s pid[%i]: assigned: jobid [%i] --> VGE jobid [%i] "  \
                              %(pname,process_id, current_unique_jobid, current_vge_jobid))
                       logger.debug("Pipeline: job contents: %s" %joblist[jobid])
                       logger.debug("Pipeline: CHECK : pid for jobid[%i] : pid (%i) <--> pid(%i) replied from VGE" \
                                     %(process_id, current_unique_jobid, check_pipeline_pid))
                       Flag=False
                #
                #

                if not flag_newjob:
                   Flag=False

          except socket.timeout:
                retry +=1
                logger.info("%s pid[%i]: connection timed out." %(pname,process_id))
                pass

          except socket.error, error:
                retry +=1
                #logger.debug("Pipeline pid(%i): connection refused (%s)"  %(process_id, error))
                time.sleep(nsleep_connection_refused)
                if "Connection refused" in error:
                     pass
          except Exception, error:
                retry +=1
                logger.error("%s pid[%i]: error [%s]"  %(pname,process_id, error))
                if "Connection refused" in error:
                     pass
          finally: 
                sock.close()
       
  #
  # critical check
  #
  #if current_unique_jobid == -1 and flag_newjob:
  #    logger.critical("%s pid[%i]: fatal error!! uid from VGE was not assigned!\n\n" %(pname,process_id, current_unique_jobid))
  #    sys.exit(1)

  time_jobdispatch=time.time() - time_jobdispatch

  #
  # sleep for a while
  #
  time.sleep(nsleep_interval)
  
  #
  # wait until the job is completed.
  #
  wait_time = time.time()
  Flag=True
  if flag_newjob:
      logger.info("%s pid[%i]: wait for jobid [%i] to finish" %(pname,process_id,current_unique_jobid))
      icount = 1
      while Flag:
              try:
                    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    sock.settimeout(timeout_ask)
                    mesg = ""
                    messeage=""
                    sock.connect((host, port))
                    #
                    jobstatus={}
                    jobstatus["request"] = current_unique_jobid
                    jobstatus["vge_jobid"] = current_vge_jobid
                    jobstatus["max_task"] = current_max_task
                    jobstatus["pipeline_pid"] = process_id
                    joblist_shipped = pickle.dumps(jobstatus)
                    # 
                    # ask VGE the status for this job
                    #
                    time_waitsend=time.time()
                    sock.sendall(joblist_shipped)
                    time_waitsend=time.time() - time_waitsend
                    time.sleep(nsleep_aftersend)
                    #
                    # receive a messeage(the job status) from VGE
                    #
                    time_waitrecv=time.time()
                    mesg = sock.recv(bufsize)
                    time_waitrecv=time.time() - time_waitrecv
                    if mesg is not None or mesg.strip() != "":
                        try: 
                              messeage=pickle.loads(mesg)
                        except EOFError: 
                              pass
                        #
                        #logger.debug(Pipeline: messeage from VGE ---> [%s]" %messeage)
                        #
                        if "done" in messeage or "aborted" in messeage:
                             temp = messeage.split(",")
                             #
                             return_code=0
                             #
                             if len(temp) > 1:
                                 #
                                 return_code = int(temp[1])
                                 joblist[jobid]["return_code"] = return_code
                                 #
                                 result = return_code
                                 #
                                 if result != 0:
                                    logger.warning("%s pid[%i]: job [id:%i] has been completed with NON-ZERO return code[%i] / inquiry count[%i]."  \
                                        %(pname,process_id, current_unique_jobid, return_code,icount))
                                 else: 
                                    logger.info("%s pid[%i]: job [id:%i] has been completed with return code[%i] / inquiry count[%i]."  \
                                        %(pname,process_id, current_unique_jobid, return_code,icount))
                                 #
                                 #
                             else: 
                                 # this statement should not be passed.
                                 logger.critical("Pipeline: error! no return code (%s)" %messeage)

                             if len(temp) > 2:
                                 logger.debug("Pipeline: check pid for jobid(%i) : pid(%i) <--> pid(%i) replied from VGE" \
                                                %(current_unique_jobid, process_id, int(temp[2])))
                             #
                             Flag=False
                             #
                        else:
                             icount += 1

              except socket.error, error:
                    if "Connection refused" in error:
                         pass
              except Exception, error:
                    logger.error("%s: check!! exception error! [%s]" %(pname,error))
                    pass
              finally: 
                    sock.close()


              #
              # sleep for a while then retry
              #
              #logger.debug("Pipeline pid(%i): wait then retry to ask." %process_id)
              time.sleep(nsleep_request)

  #
  wait_time = time.time() - wait_time
  total_time = time.time() - total_time
  #

  #
  # output job-info
  #
  if flag_newjob:
    logger.info("%s pid[%i]: job [id:%i]: elapsed times -> connect[%8.3e] job:mesg-send[%8.3e]/recv[%8.3e] job-dispatch[%8.3e] job-wait[%8.3e] total[%8.3e] sec" \
              %(pname,process_id, current_unique_jobid, time_connection, time_jobsend,time_jobrecv,time_jobdispatch, wait_time, total_time))
  else:
    logger.info("%s pid[%i]: elapsed times -> connect[%8.3e] job:mesg-send[%8.3e]/recv[%8.3e] job-dispatch[%8.3e] total[%8.3e] sec" \
              %(pname,process_id, time_connection, time_jobsend,time_jobrecv,time_jobdispatch, total_time))

  #
  # jobid increment
  #
  #jobid +=1

  #
  # waiting for a while
  #
  #logger.debug("Pipeline pid(%i): VGE is busy so Pipeline is sleeping for (%8.3e) sec."  %(process_id, nsleep_controller))
  #time.sleep(nsleep_controller)
  #

  #
  if flag_newjob:
     logger.debug("%s pid[%i]: result (newjob) .... [%i] " %(pname,process_id,result))
  else:
     logger.debug("%s pid[%i]: result .... [%s] " %(pname,process_id,result))
  #

  #
  #
  return result
  #
  #


