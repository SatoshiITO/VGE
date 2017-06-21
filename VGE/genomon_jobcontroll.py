import os, multiprocessing
import select, socket
import pickle
import random, time

from VGE.make_vge_jobstatuslist import *
from VGE.get_vge_conf import get_vge_conf

def genomon_jobcontroll(cl_args, job_args, total_joblist,new_joblist,task_check,command_list):
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
    task_check["exit"]=False
    server_connected=True
    #
    command_id=0
    jobid = 0 # is an indivitual job id used in VGE
    unique_jobid = 0 # is an unique job id for both VGE an GenomonPipeline
    genomon_pid_for_job=-1 # in case of the job that is not listed yet.
    #
    #
    mypid = os.getpid()
    logger.info("VGE(JOB): Genomon job controller [pid:%i] is running. " %mypid)

    #
    # communicate with GenomonPipeline(s)
    #
    host = socket.gethostbyname(socket.gethostname())
    port = 8000
    backlog = 128
    bufsize = 16384
    default_bufsize = bufsize
    flag_socket_error=False
    count_socket_error=0
    time_onesocket = 0.0
    genomon_connect_established = 1
    
    #
    # Config
    #
    server_timeout=600.0 # sec
    nsleep_aftersend=0.0
    nsleep_updatelist=0.0
    nsleep_afterconnectionerror=1.0
    nsleep_controller=1.0

    # read from conf
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
                  # wait for connection with GenomonPipeline(s)
                  #
                  #logger.info("VGE(JOB): try to connect with GenomonPipeline")
                  
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
                          # get a messeage from GenomonPipeline
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
                          logger.info("VGE(JOB): connection with GenomonPipeline was established. [%i] " %genomon_connect_established)
                          genomon_connect_established += 1
                                  
                          if isinstance(messeage,str) :
                               if "finished" in messeage: # the VGE controller will stop.
                                    logger.info("VGE(JOB): got a shutdown signal from GenomonPipeline.")
                                    mesg = "byebye"
                                    flag_byebye=True
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(mesg))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    time.sleep(nsleep_aftersend)
                                    server_connected=False
                                    #server_sock.close()
                               elif "hello" in messeage: # echo hello
                                    logger.info("VGE(JOB): got a start signal from GenomonPipeline.")
                                    mesg = "hello"
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(mesg))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    time.sleep(nsleep_aftersend)
                          elif isinstance(messeage,dict) :
                               #logger.debug("VGE(JOB): messeage from GenomonPipeline (%s)" %messeage)
                               #
                               # job status inquiry from GenomonPipeline
                               #
                               if messeage.has_key("request"):
                                    genomon_pid_for_job=-1 # in case of the job that is not listed yet.
                                    check_unique_jobid = int(messeage["request"]) # this should be unique_jobid
                                    check_vge_jobid = int(messeage["vge_jobid"])
                                    check_max_task = int(messeage["max_task"])
                                    check_genomon_pid = int(messeage["genomon_pid"])
                                    check_status = ""
                                    output = ""
                                    output += "VGE(JOB): jobid (%i) max_task(%i) job_check: " \
                                               %(check_unique_jobid, check_max_task)

                                    logger.info("VGE(JOB): request job status [id:%i] from GenomonPipeline [pid:%i]" %(check_unique_jobid,check_genomon_pid))
                                    #
                                    # check whether the job(s) including bulk jobs for this unique jobid were done or not
                                    #
                                    check_flag=True
                                    output_running ={}
                                    check_return_code = 0 # for bulk job, the code is the largest one.
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
                                                   logger.debug("VGE(JOB): retry to read data.") # a little bit too early to check
                                                   time.sleep(nsleep_updatelist)
                                                   pass 
                                          #
                                          # check GenomonPipeline process id
                                          #
                                          genomon_pid_for_job = check_job["genomon_pid"]
                                          logger.debug("VGE(JOB): pid(%i) for job(%i) <--> pid(%i) requested"  \
                                                       %(genomon_pid_for_job, check_jobid,check_genomon_pid) )
                                          if genomon_pid_for_job != check_genomon_pid:
                                              logger.critical("VGE(JOB): Error!!! mismatch pid!!!")
                                              exit()

                                          if check_job["status"] == "done":
                                              check_count=0
                                              check_return_code = check_job["return_code"]
                                          else:
                                              output_running[check_jobid] = check_job 
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
                                                        logger.debug("VGE(JOB): retry to read data.")
                                                        time.sleep(nsleep_updatelist)
                                                        pass 
                                                
                                                bulkjob_id = int(check_job["bulkjob_id"])
                                                #
                                                # check GenomonPipeline process id
                                                #
                                                if count ==0:
                                                   genomon_pid_for_job = check_job["genomon_pid"]
                                                   logger.debug("VGE(JOB): pid(%i) for job(%i) <-->  pid(%i) requested"  \
                                                                      %(genomon_pid_for_job, check_jobid,check_genomon_pid) )
                                                   if genomon_pid_for_job != check_genomon_pid:
                                                       logger.critical("VGE(JOB): Error!!! mismatch pid!!!")
                                                       exit()

                                                if check_job["status"] == "done":
                                                   if check_job["unique_jobid"] == check_unique_jobid:
                                                      check_count += 1
                                                      if check_return_code < check_job["return_code"]:
                                                            check_return_code = check_job["return_code"]
                                                      output += "%03i " %bulkjob_id
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
                                    # add Genomon PID for this job
                                    check_status +=","+str(genomon_pid_for_job)
                                    #
                                    # send jobstatus to GenomonPipeline
                                    #
                                    logger.info("VGE(JOB): send messeage to GenomonPipeline (%s)" %check_status)
                                    send_time = time.time()
                                    sock.sendall(pickle.dumps(check_status))
                                    send_time = time.time() - send_time
                                    sum_send_time += send_time
                                    flag_dispatch=True
                                    time.sleep(nsleep_aftersend)

                               #
                               # new job(s) from GenomonPipeline
                               #
                               else:
                                    logger.info("VGE(JOB): incoming messeage from GenomonPipeline: %s" %messeage)
                                    if messeage is not None or messeage != "":
                                          messeage["unique_jobid"] = unique_jobid # VGE will reply to Genomon via unique_jobid
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
                                          temp["genomon_pid"] = messeage["genomon_pid"]
                                        
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
                                          # update new_joblist
                                          #
                                          start_jobid=jobid
                                          if messeage["max_task"] == 0:
                                               filename = basefilename+".sh.0"
                                               messeage["filename"]=filename
                                               messeage["bulkjob_id"]=0 # 
                                               new_joblist[jobid]=dict()
                                               new_joblist[jobid]=messeage
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
                                                     jobid +=1

                                          #
                                          answer=dict()
                                          answer["unique_jobid"] = current_unique_jobid
                                          answer["genomon_pid"] = messeage["genomon_pid"]
                                          answer["vge_jobid"] = start_jobid # this is vge_jobid which GenomonPipeline wants to know
                                          #
                                          # send the unique jobid to the worker
                                          #
                                          send_time = time.time()
                                          sock.sendall(pickle.dumps(answer))
                                          send_time = time.time() - send_time
                                          flag_dispatch=True
                                          sum_send_time += send_time
                                          time.sleep(nsleep_aftersend)
                                          logger.info("VGE(JOB): reply to GenomonPipeline: [%s] " %answer )
                                          # 
                                          #
                                    else:
                                          #
                                          # send the unique jobid to GenomonPipeline
                                          #
                                          send_time = time.time()
                                          sock.sendall(pickle.dumps(unique_jobid))
                                          send_time = time.time() - send_time
                                          sum_send_time += send_time
                                          time.sleep(nsleep_aftersend)

                          #  
                          #  close connection with GenomonPipeline
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
                         logger.info("VGE(JOB): communication with Genomon: elapsed times -> mesg-recv[%8.3e]/send[%8.3e] 1-socket cycle[%8.3e] sec" \
                                     %(recv_time, send_time, time_onesocket))
                      elif socket_count > 1:
                         logger.info("VGE(JOB): communication with %i Genomon(s) : averaged elapsed times -> mesg-recv[%8.3e]/send[%8.3e] 1-socket cycle[%8.3e] sec" \
                                     %(socket_count, sum_recv_time/socket_count, sum_send_time/socket_count, time_onesocket))
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
    # all jobs were finished so genomon_jobcontoller is terminating now.
    #
    task_check["exit"]=True # this is a shutdown signal for the MPI job controller
    logger.info("VGE(JOB): Genomon job controller [pid:%i] finished normally. " %mypid)
    #
    #
    #

    return 0

