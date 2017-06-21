import socket, pickle
import time, datetime
import subprocess, multiprocessing
import random, os ,sys
import argparse

from VGE.make_genomon_jobstatuslist import *
from VGE.get_vge_conf import *

def vge_task(arg1,arg2,arg3,arg4):

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
  #           
  total_time = time.time()
  # 

  #
  # socket
  #
  host = socket.gethostbyname(socket.gethostname())
  port = 8000
  bufsize = 16384

  #
  #  get current process id
  #  
  process_id = os.getpid()
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
  nsleep_interval = get_vge_conf("genomon","socket_interval_after",nsleep_interval)
  nsleep_request = get_vge_conf("genomon","socket_interval_request", nsleep_request)
  nsleep_connection_refused = get_vge_conf("genomon","socket_interval_error", nsleep_connection_refused)
  nsleep_aftersend = get_vge_conf("genomon","socket_interval_send", nsleep_aftersend)
  nsleep_updatelist = get_vge_conf("genomon","socket_interval_update",nsleep_updatelist)
  nsleep_controller = get_vge_conf("genomon","socket_interval_close",nsleep_controller)
  timeout_client = get_vge_conf("genomon","socket_timeout1", timeout_client)
  timeout_ask = get_vge_conf("genomon","socket_timeout2", timeout_ask)
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

  verbose=0
  verbose = int(get_vge_conf("genomon","verbose",verbose))
  from logging import getLogger,StreamHandler,basicConfig,DEBUG,INFO,WARNING,ERROR,CRITICAL
  logger=getLogger(__name__)
  if verbose == 0:
     logger.setLevel(CRITICAL)
  elif verbose == 1:
     logger.setLevel(INFO)
  elif verbose == 3:
     logger.setLevel(DEBUG)
  basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')

  logger.debug("Genomon pid[%i]: socket host=%s" %(process_id,host))
  logger.debug("Genomon pid[%i]: socket port=%i" %(process_id,port))
  logger.debug("Genomon pid[%i]: verbose level=%i" %(process_id,verbose))
  logger.debug("Genomon pid[%i]: socket buffer size=%i" %(process_id,bufsize))
  logger.debug("Genomon pid[%i]: socket timeout1 (job sending)=%12.5e" %(process_id,timeout_client))
  logger.debug("Genomon pid[%i]: socket timeout2 (job waiting)=%12.5e" %(process_id,timeout_ask))
  logger.debug("Genomon pid[%i]: socket_interval_after=%12.5e" %(process_id, nsleep_interval))
  logger.debug("Genomon pid[%i]: socket_interval_send=%12.5e" %(process_id,nsleep_aftersend))
  logger.debug("Genomon pid[%i]: socket_interval_request=%12.5e" %(process_id, nsleep_request))
  logger.debug("Genomon pid[%i]: socket_interval_error=%12.5e" %(process_id,nsleep_connection_refused))
  logger.debug("Genomon pid[%i]: socket_interval_update=%12.5e" %(process_id, nsleep_updatelist))
  logger.debug("Genomon pid[%i]: socket_interval_close=%12.5e" %(process_id, nsleep_controller))
  logger.debug( "Genomon pid[%i]: config: elapsed time=%12.5e" %(process_id, time_config))

  #
  # initialize parameters
  # 
  jobid =0
  joblist ={}
  messeage=""
  flag_genomon_task_loop=True
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
  joblist[jobid]=make_genomon_jobstatuslist(0)
  joblist[jobid]["genomon_pid"] = process_id
 
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
  # startup or shutdown VGE?
  #
  flag_newjob=True
  #
  if arg1 == "finished":
      logger.info("Genomon pid[%i]: shutdown VGE..." %process_id)
      joblist = "finished" 
      flag_newjob=False
      joblist_shipped = pickle.dumps(joblist)
  elif arg1 == "hello":
      logger.info("Genomon pid[%i]: start up VGE..." %process_id)
      joblist = "hello" 
      flag_newjob=False
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
                   logger.info("Genomon pid[%i]: socket error [%s]." %(process_id,error))

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
                    #logger.info("Genomon pid(%i): try to connect with VGE." %process_id)
                #
                sock.connect((host, port))
                logger.info("Genomon pid[%i]: connection with VGE was established. [access times:%i] " %(process_id,retry))
                time_connection = time.time() -time_connection

                if isinstance(joblist, dict):
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
                logger.info("Genomon pid[%i]: new job has been submitted to VGE. [%s]" %(process_id, pickle.loads(joblist_shipped)))
                try: 
                    time_jobsend = time.time()
                    #logger.info("check...")
                    #logger.info(joblist_shipped)
                    sock.sendall(joblist_shipped)
                    time_jobsend = time.time() - time_jobsend
                    time.sleep(nsleep_aftersend)
                except socket.error:
                    logger.critical("GenomonPipeline: Error!! socket sending was failed. [data size->(%i) byte :(%s)]" %(len(joblist_shipped), pickle.loads(joblist_shipped)))
                    pass
                    #sys.exit()
                except Exception,error:
                    logger.debug("Genomon pid[%i]: data send error (%s)"  %(process_id, error))
                    pass
                #
                # receive an incoming messeage(the jobid for this order) from VGE
                #
                time_jobrecv = time.time()
                mesg = sock.recv(bufsize)
                time_jobrecv = time.time() - time_jobrecv
                if isinstance(mesg,str):
                    messeage=None
                    try: 
                          messeage=pickle.loads(mesg)
                          logger.info("Genomon pid[%i]: response from VGE: %s"  %(process_id, messeage))
                    except EOFError: 
                          pass
                    #
                    if isinstance(messeage, dict):
                       current_unique_jobid = int(messeage["unique_jobid"])
                       current_vge_jobid = int(messeage["vge_jobid"])
                       current_max_task = int(joblist[jobid]["max_task"])
                       check_genomon_pid = int(messeage["genomon_pid"])
                       joblist[jobid]["unique_jobid"] = current_unique_jobid
                       joblist[jobid]["vge_jobid"] = current_vge_jobid
                       #
                       logger.info("Genomon pid[%i]: assigned: jobid [%i] --> VGE jobid [%i] "  \
                              %(process_id, current_unique_jobid, current_vge_jobid))
                       logger.debug("Genomon: job contents: %s" %joblist[jobid])
                       logger.debug("Genomon: CHECK : pid for jobid[%i] : pid (%i) <--> pid(%i) replied from VGE" \
                                     %(process_id, current_unique_jobid, check_genomon_pid))
                       Flag=False
                #
                #

                if not flag_newjob:
                   Flag=False

          except socket.timeout:
                retry +=1
                logger.info("Genomon pid[%i]: connection timed out." %process_id)
                pass

          except socket.error, error:
                retry +=1
                #logger.debug("Genomon pid(%i): connection refused (%s)"  %(process_id, error))
                time.sleep(nsleep_connection_refused)
                if "Connection refused" in error:
                     pass
          except Exception, error:
                retry +=1
                logger.error("Genomon pid[%i]: error [%s]"  %(process_id, error))
                if "Connection refused" in error:
                     pass
          finally: 
                sock.close()
       
  #
  # critical check
  #
  if current_unique_jobid == -1 and flag_newjob:
      logger.critical("\n\nGenomon pid[%i]: fatal error!! uid from VGE was not assigned!\n\n" %(process_id, current_unique_jobid))
      sys.exit()

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
      logger.info("Genomon pid[%i]: wait for jobid [%i] to finish" %(process_id,current_unique_jobid))
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
                    jobstatus["genomon_pid"] = process_id
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
                        if "done" in messeage:
                             temp = messeage.split(",")
                             if len(temp) > 1:
                                 return_code = int(temp[1])
                                 joblist[jobid]["return_code"] = return_code
                             else: 
                                 logger.critical("GenomonPipeline: error! no return code (%s)" %messeage)

                             if len(temp) > 2:
                                 logger.debug("GenomonPipeline: check pid for jobid(%i) : pid(%i) <--> pid(%i) replied from VGE" \
                                                %(current_unique_jobid, process_id, int(temp[2])))

                             logger.info("Genomon pid[%i]: job [id:%i] has been completed with return code[%i] / inquiry count[%i]."  \
                                    %(process_id, current_unique_jobid, return_code,icount))
                             Flag=False
                        else:
                             icount += 1

              except socket.error, error:
                    if "Connection refused" in error:
                         pass
              except Exception, error:
                    logger.error("GenomonPipeline: check!! exception error! [%s]" %error)
                    pass
              finally: 
                    sock.close()


              #
              # sleep for a while then retry
              #
              #logger.debug("Genomon pid(%i): wait then retry to ask." %process_id)
              time.sleep(nsleep_request)

  wait_time = time.time() - wait_time
  total_time = time.time() - total_time
  if flag_newjob:
    logger.info("Genomon pid[%i]: job [id:%i]: elapsed times -> connect[%8.3e] job:mesg-send[%8.3e]/recv[%8.3e] job-dispatch[%8.3e] job-wait[%8.3e] total[%8.3e] sec" \
              %(process_id, current_unique_jobid, time_connection, time_jobsend,time_jobrecv,time_jobdispatch, wait_time, total_time))
  else:
    logger.info("Genomon pid[%i]: elapsed times -> connect[%8.3e] job:mesg-send[%8.3e]/recv[%8.3e] job-dispatch[%8.3e] total[%8.3e] sec" \
              %(process_id, time_connection, time_jobsend,time_jobrecv,time_jobdispatch, total_time))


  # jobid increment
  #jobid +=1
  #
  # waiting for a while
  #
  #logger.debug("Genomon pid(%i): VGE is busy so GenomonPipeline is sleeping for (%8.3e) sec."  %(process_id, nsleep_controller))
  #time.sleep(nsleep_controller)
  #

  return




