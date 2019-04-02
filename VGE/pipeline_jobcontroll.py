#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */

import os, multiprocessing
import select, socket
import pickle
import random, time

from VGE.make_vge_jobstatuslist import *
from VGE.get_vge_conf import get_vge_conf

def pipeline_jobcontroll(cl_args, job_args, total_joblist,new_joblist,task_check, pipeline_parent_pid_list, command_list):
    #
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
    elif verbose == 1 or verbose ==2:
        logger.setLevel(INFO)
    elif verbose ==3:
        logger.setLevel(DEBUG)
    basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')

    #
    # initialize...
    #
    flag_byebye=False
    flag_forcedbyebye=False
    task_check["exit"]=False
    task_check["forced"]=False
    task_check["restofjob"]=0
    server_connected=True
    #
    command_id=0
    jobid = 0 # is an indivitual job id used in VGE
    unique_jobid = 0 # is an unique job id for both VGE an Pipeline
    pipeline_pid_for_job=-1 # in case of the job that is not listed yet.
    #
    #
    mypid = os.getpid()
    logger.info("VGE(JOB): Pipeline job controller [pid:%i] is running. " %mypid)
    #
    #pipeline_parent_pid_list={}  # pipeline_parent_pid_list[25434] = True if VGE got a final signal from Pipeline)
    #
    num_of_pipeline_sample=0
    unique_pipeline_parent_id=[] # unique_pipeline_parent_id[1] = 25434
    #

    #
    # communicate with Pipeline(s)
    #
    host = socket.gethostbyname(socket.gethostname())
    port = 8000
    backlog = 128
    bufsize = 16384
    default_bufsize = bufsize
    flag_socket_error=False
    count_socket_error=0
    time_onesocket = 0.0
    pipeline_connect_established = 1
    flag_dispatch=False
    
    #
    # Config
    #
    server_timeout=600.0 # sec
    nsleep_aftersend=0.0
    nsleep_updatelist=0.0
    nsleep_afterconnectionerror=1.0
    nsleep_controller=1.0

    #
    # read from conf
    #
    if job_args["flag_vge_conf"]:
       port =  int(get_vge_conf("socket","port",port))
       bufsize = int(get_vge_conf("socket","bufsize",bufsize))
       server_timeout = get_vge_conf("vge","socket_timeout",server_timeout)
       nsleep_aftersend = get_vge_conf("vge","socket_interval_send",nsleep_aftersend)
       nsleep_updatelist = get_vge_conf("vge","socket_interval_update",nsleep_updatelist)
       nsleep_afterconnectionerror = get_vge_conf("vge","socket_interval_error",nsleep_afterconnectionerror)
       nsleep_controller = get_vge_conf("vge","socket_interval_close",nsleep_controller)
    #
    logger.info("VGE(JOB): socket host = %s" %host)
    logger.info("VGE(JOB): socket port = %i" %port)
    logger.info("VGE(JOB): socket buffer size = %i" %bufsize)
    logger.info("VGE(JOB): socket_interval_send=%12.5e" %nsleep_aftersend)
    logger.info("VGE(JOB): socket_interval_update=%12.5e" %nsleep_updatelist)
    logger.info("VGE(JOB): socket_interval_close=%12.5e" %nsleep_afterconnectionerror)
    logger.info("VGE(JOB): socket_timeout=%12.5e" %server_timeout)
    logger.info("VGE(JOB): socket_interval_close=%12.5e" %nsleep_controller)

    #
    while True:
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        inputs = set([server_sock])
        if not server_connected:
              break
        try:
             server_sock.bind((host, port))
             server_sock.listen(backlog)
             #
             if flag_socket_error == True:
                  logger.info("VGE(JOB): ....recovered (%i)" %count_socket_error)
                  flag_socket_error =False
                  count_socket_error=0
             #
             while True:
                  #
                  # wait for connection with Pipeline(s)
                  #
                  #logger.info("VGE(JOB): try to connect with Pipeline")
                  
                  try: 
                      inputready, outputready, exceptready = select.select(inputs, [], [],server_timeout)
                  except Exception,error:
                      logger.debug("VGE(JOB): check. connection error. [%s] " %error)

                  if not inputready:
                     logger.info("VGE(JOB): connection timed out. [%i sec] " %server_timeout)
                  else:
                     pass

                  sum_send_time=0.0
                  sum_recv_time=0.0
                  socket_count=0
                  for sock in inputready:
                      time_onesocket = time.time()
                      flag_dispatch=False
                      if sock is server_sock:
                          client, address = server_sock.accept()
                          inputs.add(client)
                      else:
                          #
                          # get a messeage from Pipeline
                          #
                          socket_count  += 1
                          messeage=""
                          mesg=""
                          recv_time = time.time()
                          temp = None
                          #
                          #mesg=sock.recv(bufsize)
                          icount=0
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
                          #
                          #logger.info("VGE(JOB): recv mesg size = %i [%s] " %(len(mesg),mesg))
                          #logger.info("VGE(JOB): recv mesg size=%i" %(len(mesg)))
                          recv_time = time.time() - recv_time
                          sum_recv_time += recv_time

                          if mesg > 0:
                              logger.info("VGE(JOB): recieved mesg size=%i [%i recv times]" %(len(mesg), icount))
                          try: 
                                  messeage=pickle.loads(mesg)
                          except EOFError:
                                  pass
                          except ValueError:
                                  logger.error("VGE(JOB): Value error. mesg is not recieved correctly. [bufsize:%i] [mesg:%s]" %(bufsize,mesg))
                          except Exception,error:
                                  logger.error("VGE(JOB): exception error.... check [%s]" %(error))
  
                          #
                          logger.info("VGE(JOB): connection with Pipeline was established. [%i] " %pipeline_connect_established)
                          pipeline_connect_established += 1
                                  
                          if isinstance(messeage,str) :
                               if "finished" in messeage: # the VGE controller will stop.
                                    logger.info("VGE(JOB): got a shutdown signal from Pipeline/VGE_CONNECT.")
                                    mesg = "byebye"
                                    flag_byebye=True
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(mesg))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    time.sleep(nsleep_aftersend)
                                    server_connected=False
                                    #server_sock.close()

                               elif "forcedstop" in messeage: # the VGE controller will be force to stop.
                                    logger.info("VGE(JOB): got a forced shutdown signal from Pipeline/VGE_CONNECT.")
                                    mesg = "byebye!!!"
                                    flag_byebye=True
                                    flag_forcedbyebye=True
                                    #
                                    task_check["forced"]=True # this is a forced shutdown signal for the MPI job controller
                                    #
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(mesg))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    time.sleep(nsleep_aftersend)
                                    server_connected=False
                                    #server_sock.close()

                               elif "hello" in messeage: # echo hello
                                    logger.info("VGE(JOB): got a start signal from Pipeline.")
                                    mesg = "hello"
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(mesg))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    time.sleep(nsleep_aftersend)

                               elif "pipeline_ppid_list" in messeage: #  request Pipeline parent process id list
                                    logger.info("VGE(JOB): request Pipeline parent process id list from vge_connect.")
                                    mesg = ""
                                    temp = pipeline_parent_pid_list.copy() # this copy is a must because pipeline_parent_pid_list is a multiprocessing module object.
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(temp))
                                    send_time = time.time() - send_time
                                    del temp
                                    sum_send_time += send_time
                                    time.sleep(nsleep_aftersend)

                               elif "restofjob" in messeage: #  request Pipeline parent process id list
                                    logger.info("VGE(JOB): request the number of rest of jobs to be carried out from VGE_CONNECT")
                                    mesg = ""
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(task_check["restofjob"]))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    time.sleep(nsleep_aftersend)
                               
                          elif isinstance(messeage,dict) :
                               logger.debug("VGE(JOB): messeage from Pipeline (%s)" %messeage)

                               #
                               # mark pipeline byebye flag
                               #
                               if messeage.has_key("byebye_from_pipeline"):
                                  temp =  int(messeage["pipeline_parent_pid"])
                                  if  pipeline_parent_pid_list.has_key(temp):
                                      logger.info("VGE(JOB): Pipeline [pid: %i] was completed." %temp)
                                      pipeline_parent_pid_list[temp]=True
                                  else:
                                      logger.info("VGE(JOB): Pipeline [pid: %i] was not found in pipeline_parent_pid_list [%s]." %(temp,pipeline_parent_pid_list))
                               #
                               # mark a new pipeline 
                               #
                               elif messeage.has_key("hello_from_pipeline"):
                                  temp =  int(messeage["pipeline_parent_pid"])
                                  if  pipeline_parent_pid_list.has_key(temp):
                                      logger.info("VGE(JOB): we know this Pipeline [pid: %i].... hello again?" %(temp))
                                      logger.debug("VGE(JOB): pipeline_parent_pid_list ...%s" %pipeline_parent_pid_list)
                                  else:
                                      logger.info("VGE(JOB): got a new Pipeline parent pid (got a hello pipeline) [%i]." %temp)
                                      #
                                      # update Pipeline parent list
                                      pipeline_parent_pid_list[temp]=False
                                      num_of_pipeline_sample += 1
                                      unique_pipeline_parent_id.append(temp)
                                      logger.info("VGE(JOB): [update] current num of parent pids Pipelines are running is %i ." %len(pipeline_parent_pid_list.keys()))

                               #
                               # job status inquiry from Pipeline
                               #
                               elif messeage.has_key("request"):
                                    pipeline_pid_for_job=-1 # in case of the job that is not listed yet.
                                    check_unique_jobid = int(messeage["request"]) # this should be unique_jobid
                                    check_vge_jobid = int(messeage["vge_jobid"])
                                    check_max_task = int(messeage["max_task"])
                                    check_pipeline_pid = int(messeage["pipeline_pid"])
                                    check_status = ""
                                    output = ""
                                    output += "VGE(JOB): jobid (%i) max_task(%i) job_check: " \
                                               %(check_unique_jobid, check_max_task)

                                    logger.info("VGE(JOB): request job status [id:%i] from Pipeline [pid:%i]" %(check_unique_jobid,check_pipeline_pid))
                                    #
                                    # check whether the job(s) including bulk jobs for this unique jobid were done or not
                                    #
                                    check_flag=True
                                    #output_running ={}
                                    #check_return_code = 0 # # for bulk job, the code is the largest one.
                                    check_return_code = 0 # for bulk job, returned the last non-zero value if it was found.
                                    #
                                    check_jobid = int(check_vge_jobid)
                                    if check_max_task == 0: 
                                       check_count=-1
                                       if total_joblist.has_key(check_jobid):
                                          flag=True
                                          while flag:
                                              try: 
                                                  check_job = total_joblist[check_jobid]
                                                  flag=False
                                              except KeyError:
                                                   logger.debug("VGE(JOB): (Key) retry to read data.") # a little bit too early to check
                                                   time.sleep(nsleep_updatelist)
                                                   pass 
                                              except EOFError:
                                                   #logger.debug("VGE(JOB): (EOF) retry to read data.") # a little bit too early to check
                                                   time.sleep(nsleep_updatelist)
                                                   pass 
                                          #
                                          # check Pipeline process id
                                          #
                                          pipeline_pid_for_job = check_job["pipeline_pid"]
                                          logger.debug("VGE(JOB): pid(%i) for job(%i) <--> pid(%i) requested"  \
                                                       %(pipeline_pid_for_job, check_jobid,check_pipeline_pid) )
                                          #if pipeline_pid_for_job != check_pipeline_pid:
                                          #    logger.critical("VGE(JOB): Error!!! Error!!! mismatch pid!!! something wrong!!!!")
                                          #    exit()

                                          if check_job["status"] == "done" or check_job["status"] == "aborted":
                                              check_count=0
                                              temp_check_return_code = int(check_job["return_code"])
                                              if temp_check_return_code != 0:
                                                   check_return_code = temp_check_return_code
                                                   logger.warning("VGE(JOB): job id[%i] has a non-zero return code[%i] " %(check_unique_jobid, check_return_code))
                                              del temp_check_return_code
                                              #
                                          else:
                                              #output_running[check_jobid] = check_job 
                                              pass # the job is not carried out yet.
                                    else: 
                                       check_count = 0
                                       count = 0
                                       if total_joblist.has_key(check_jobid):
                                          while check_max_task > count: 
                                                check_job = dict()
                                                flag=True
                                                while flag:
                                                   try: 
                                                        check_job = total_joblist[check_jobid]
                                                        flag=False
                                                   except KeyError:
                                                        logger.debug("VGE(JOB): (Key) retry to read data.")
                                                        time.sleep(nsleep_updatelist)
                                                        pass 
                                                   except EOFError:
                                                        #logger.debug("VGE(JOB): (EOF) retry to read data.")
                                                        time.sleep(nsleep_updatelist)
                                                        pass 
                                                
                                                bulkjob_id = int(check_job["bulkjob_id"])
                                                #
                                                # check Pipeline process id
                                                #
                                                if count ==0:
                                                   pipeline_pid_for_job = check_job["pipeline_pid"]
                                                   logger.debug("VGE(JOB): pid(%i) for job(%i) <-->  pid(%i) requested"  \
                                                                      %(pipeline_pid_for_job, check_jobid,check_pipeline_pid) )
                                                   #if pipeline_pid_for_job != check_pipeline_pid:
                                                   #    logger.critical("VGE(JOB): Error!!! mismatch pid 2!!!")
                                                   #    exit()

                                                if check_job["status"] == "done" or check_job["status"] == "aborted":
                                                   if check_job["unique_jobid"] == check_unique_jobid:
                                                      #
                                                      check_count += 1
                                                      output += "%03i " %bulkjob_id
                                                      #
                                                      temp_return_code = int(check_job["return_code"])
                                                      if temp_return_code !=0 :
                                                            logger.warning("VGE(JOB): job id[%i] (bulkjob id[%i]) has a non-zero return code[%s] " %(check_unique_jobid, bulkjob_id,temp_return_code))
                                                            check_return_code = temp_return_code
                                                      #
                                                   else:
                                                      # this statement should not be passed.
                                                      logger.critical("VGE(JOB) something wrong!! check_jobid   (%i)" %check_jobid)
                                                      logger.critical("VGE(JOB) something wrong!! check_maxtask (%i)" %check_max_task)
                                                      logger.critical("VGE(JOB) something wrong!! check_job     (%s)" %check_job)
                                                      for okey, ojob in total_joblist.items():
                                                          print "total_list: jobid(%04i) unique_jobid(%04i) bulk_id(%04i)" \
                                                                 %(okey, ojob["unique_jobid"], ojob["bulkjob_id"])
                                                      time.sleep(30)
                                                else: 
                                                      # some jobs are still running.
                                                      pass
                                                #
                                                check_jobid +=1
                                                count +=1


                                    if check_count == check_max_task: # all jobs for this unique id 
                                          output += "\n"
                                          logger.debug(output)
                                          output = "VGE(JOB): CHECK: job [id:%i] has been completed. [total %i]" %(check_unique_jobid, check_max_task)
                                          logger.info(output)
                                          check_status = "done,"+ str(check_return_code)  # done,0 for normally finished job
                                    else:
                                          output += "\n"
                                          logger.debug(output)
                                          output = "VGE(JOB): CHECK: job [id:%i] is not done yet. [total:%i  completed:%i  queued:%i]" \
                                                      %(check_unique_jobid, check_max_task, check_count, (check_max_task - check_count))
                                          logger.info(output)
                                          check_status = "wait,0"
                                    # 
                                    # add Pipeline PID for this job
                                    check_status +=","+str(pipeline_pid_for_job)
                                    #
                                    # send jobstatus to Pipeline
                                    #
                                    logger.info("VGE(JOB): send messeage to Pipeline (%s)" %check_status)
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(check_status))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    flag_dispatch=True
                                    time.sleep(nsleep_aftersend)

                               #
                               # new job(s) from Pipeline
                               #
                               else:
                                    logger.info("VGE(JOB): incoming messeage from Pipeline: %s" %messeage)
                                    if messeage is not None or messeage != "":
                                          messeage["unique_jobid"] = unique_jobid # VGE will reply to Pipeline via unique_jobid
                                          messeage.update(make_vge_jobstatuslist())

                                          #
                                          # update command list
                                          #
                                          current_unique_jobid=unique_jobid
                                          unique_jobid +=1
                                          command_list[command_id]=dict()
                                          temp=dict()
                                          temp["command"] = messeage["command"]
                                          temp["basefilename"] = messeage["basefilename"]
                                          temp["max_task"] = messeage["max_task"]
                                          temp["unique_jobid"] = current_unique_jobid
                                          temp["pipeline_pid"] = messeage["pipeline_pid"]
                                          temp["pipeline_parent_pid"] = messeage["pipeline_parent_pid"]
                                        
                                          #
                                          basefilename = messeage["basefilename"]
                                          #

                                          command_list[command_id]=temp
                                          messeage["command_id"] = command_id
                                          command_id += 1
                                          del temp
                                          del messeage["command"]
                                          del messeage["basefilename"]

                                          #logger.debug("VGE(JOB): update command_list: %s" %command_list)

                                          #
                                          # check if pipeline parent pid is new
                                          #
                                          temp = int(messeage["pipeline_parent_pid"])
                                          if not pipeline_parent_pid_list.has_key(temp):
                                             #
                                             # update Pipeline parent list
                                             #
                                             logger.info("VGE(JOB): a job from new Pipeline [pid %i] has come." %temp)
                                             pipeline_parent_pid_list[temp]=False
                                             num_of_pipeline_sample += 1
                                             unique_pipeline_parent_id.append(temp)
                                             logger.info("VGE(JOB): [update] num of parent pids Pipelines are running is %i ." %len(pipeline_parent_pid_list.keys()))
                                          else:
                                             logger.debug("VGE(JOB): we already know this Pipeline process id [%i]." %temp)

                                          #
                                          # update new_joblist
                                          #
                                          start_jobid=jobid
                                          if messeage["max_task"] == 0:
                                               filename = basefilename+".sh.0"
                                               messeage["filename"]=filename
                                               messeage["bulkjob_id"]=0 # 
                                               new_joblist[jobid]=dict()
                                               new_joblist[jobid]=messeage
                                               #
                                               total_joblist[jobid]=dict()
                                               total_joblist[jobid]=messeage
                                               #
                                               #time.sleep(nsleep_updatelist)
                                               jobid +=1
                                          else:  # bulk job
                                               number_of_bulkjob = int(messeage["max_task"])
                                               for bulkjob_id in range(number_of_bulkjob):
                                                     filename = basefilename+".sh.%i" %bulkjob_id
                                                     messeage["filename"]=filename
                                                     messeage["bulkjob_id"]=bulkjob_id
                                                     new_joblist[jobid]=dict()
                                                     new_joblist[jobid]=messeage
                                                     #
                                                     total_joblist[jobid]=dict()
                                                     total_joblist[jobid]=messeage
                                                     #
                                                     jobid +=1

                                          #
                                          answer=dict()
                                          answer["unique_jobid"] = current_unique_jobid
                                          answer["pipeline_pid"] = messeage["pipeline_pid"]
                                          answer["pipeline_parent_pid"] = messeage["pipeline_parent_pid"]
                                          answer["vge_jobid"] = start_jobid # this is vge_jobid which Pipeline wants to know
                                          #
                                          # send the unique jobid to the worker
                                          #
                                          send_time = time.time()
                                          sock.sendall(pickle.dumps(answer))
                                          send_time = time.time() - send_time
                                          flag_dispatch=True
                                          sum_send_time += send_time
                                          time.sleep(nsleep_aftersend)
                                          logger.info("VGE(JOB): reply to Pipeline: [%s] " %answer )
                                          # 
                                          #
                                    else:
                                          #
                                          # send the unique jobid to Pipeline
                                          #
                                          send_time = time.time()
                                          sock.sendall(pickle.dumps(unique_jobid))
                                          send_time = time.time() - send_time
                                          sum_send_time += send_time
                                          time.sleep(nsleep_aftersend)

                          #  
                          #  close connection with Pipeline
                          # 
                          sock.close()
                          inputs.remove(sock)
                          if flag_byebye:
                                break
                  #
                  #
                  time_onesocket = time.time() - time_onesocket
                  if flag_dispatch:
                      if socket_count == 1: 
                         logger.info("VGE(JOB): communication with Pipeline: elapsed times -> mesg-recv[%8.3e]/send[%8.3e] 1-socket cycle[%8.3e] sec" \
                                     %(recv_time, send_time, time_onesocket))
                      #
                      elif socket_count > 1:
                         logger.info("VGE(JOB): communication with %i Pipeline(s) : averaged elapsed times -> mesg-recv[%8.3e]/send[%8.3e] 1-socket cycle[%8.3e] sec" \
                                     %(socket_count, sum_recv_time/socket_count, sum_send_time/socket_count, time_onesocket))
                      #
                  if flag_byebye:
                      break

        except socket.error, error:
             if flag_socket_error==False:
                   logger.info("VGE(JOB): sokcket connection failed.")
             flag_socket_error=True
             count_socket_error +=1
             time.sleep(nsleep_afterconnectionerror)
             pass

        #except Exception, error:
        #     logger.debug("VGE(JOB): error was occured.... [%s]" %error)
        #     pass

        finally:
             for sock in inputs:
                  sock.close()
             if flag_byebye:
                 break

        #
        # sleep server 
        #
        if not flag_socket_error:
           logger.info("VGE(JOB): server closed.")
           logger.info("VGE(JOB): sleep for [%4.1f] sec" %nsleep_controller)
           pass
        time.sleep(nsleep_controller)
        server_sock.close()
        bufsize= default_bufsize
        if flag_byebye:
            break

    #
    # all jobs were finished so pipeline_jobcontoller is terminating now.
    #
    
    logger.info("VGE(JOB): check pipeline_parent_pid_list... %s " %pipeline_parent_pid_list)
    #
    task_check["exit"]=True # this is a shutdown signal for the MPI job controller
    #
    if flag_forcedbyebye:
       logger.info("VGE(JOB): Pipeline job controller [pid:%i] was forced to be finished." %mypid)
    else:
       logger.info("VGE(JOB): Pipeline job controller [pid:%i] finished normally. " %mypid)
    #
    #
    #

    return 0

