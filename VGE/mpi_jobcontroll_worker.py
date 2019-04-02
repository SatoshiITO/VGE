#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
from mpi4py import MPI
import os,signal
import commands
import math, datetime, time
from logging import getLogger,StreamHandler,basicConfig,DEBUG,INFO,WARNING,ERROR,CRITICAL
import subprocess
#
from VGE.get_vge_conf import get_vge_conf
#from VGE.get_masterstatus import get_masterstatus

def mpi_jobcontroll_worker(cl_args, mpi_args, comm):

    #
    # master rank
    #
    master_rank=0

    #
    # MPI message tag
    #
    message_tag=10      # job message from the master
    forcedstop_tag=99   # forced stop message from the master
    
    #
    flag_forcedstop=False

    # 
    # set verbose level
    #
    #
    logger=getLogger(__name__)
    logger.setLevel(INFO)
    #
    verbose = 0
    verbose = int(get_vge_conf("vge","verbose",verbose))
    if verbose == 0:
        logger.setLevel(CRITICAL)
    elif verbose == 1:
        logger.setLevel(INFO)
    elif verbose == 2:
        logger.setLevel(CRITICAL)
    elif verbose == 3:
        logger.setLevel(DEBUG)
    basicConfig(format='%(asctime)s:%(levelname)s:%(message)s')
    #
    myrank = comm.Get_rank()
    nproc = comm.Get_size()
    number_of_nproc_digit = int(math.log10(nproc)+1)
    myrank_str=str(myrank).zfill(number_of_nproc_digit)
    status=MPI.Status()
    #
    max_sizecommand = 131071 # for generic linux. for Kei, the max size is 262143 bye (less than 256KB).
    max_sizecommand = int(get_vge_conf("vge","mpi_command_size",max_sizecommand))
    logger.info("VGE(MPI): worker[%s]: max. command size = %i" %(myrank_str, max_sizecommand))

    # job status
    jobstatus = None
    logger.info("VGE(MPI): MPI worker[%s] is running." %myrank_str)
    command = ""
    ret = 0 # return code from subprocess

    # probe parameters ; not used in workers
    #number_of_probe = 1000
    #nsleep_probe=0.01

    # monitoring interval of  mpi_test() on worker for killing a job. this is available if monitor_worker_runjob option is on.
    time_interval_irecv_test = 30.0
    time_interval_irecv_test = float(get_vge_conf("vge","worker_interval_irecv_test",time_interval_irecv_test))

    # pause after checking a running job [sec]. this is available if monitor_worker_runjob option is on.
    time_interval_runjobcheck=10.0
    time_interval_runjobcheck = float(get_vge_conf("vge","worker_interval_runjobcheck",time_interval_runjobcheck))

    # monitoring running job on worker for emergency call from master ?
    flag_monitor_worker_runjob=False
    flag_killthejob=False
    if cl_args.monitor_workerjob:
       flag_monitor_worker_runjob=True
       logger.info("VGE(MPI): MPI worker[%s] will monitor a job when it is running." %myrank_str)
       logger.info("VGE(MPI): MPI worker[%s] [job monitoring] worker_interval_irecv_test : [%0.2f sec]." %(myrank_str,time_interval_irecv_test))
       logger.info("VGE(MPI): MPI worker[%s] [job monitoring] worker_interval_runjobcheck: [%0.2f sec]." %(myrank_str,time_interval_runjobcheck))
       #
       # monitor a forced-stop signal of a running job ...
       #
       emergency_signal_recv= comm.irecv(source=master_rank, tag=forcedstop_tag)
       #

    #//////////////////////////
    # job loop for worker nodes
    #//////////////////////////
    while True:
    
        #
        # request a job and send the previous job status to the master
        #
        start_job=time.time()

        start_time=time.time()
        #comm.send(jobstatus, dest=0, tag=myrank) 
        comm.send(jobstatus, dest=0, tag=message_tag) 
        end_time=time.time()
        mpi_send_time=end_time - start_time
        if jobstatus is not None:
            logger.info("VGE(MPI): worker[%s]: send job status to the master: [%s]" %(myrank_str, jobstatus))

        #
        # init
        #
        flag_forcedstop=False

        #
        # get an order from the master 
        #
        start_time=time.time()
        #package = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        package = comm.recv(source=0, tag=message_tag, status=status)
        end_time=time.time()
        mpi_recv_time=end_time - start_time

        command=None
        filename=None
        jobid=None
        bulkjob_id=None
        if isinstance(package,dict):
            command = package["command"]
            filename = package["filename"]
            jobid  = package["jobid"]
            bulkjob_id = package["bulkjob_id"]
            if command is not None:
                logger.info("VGE(MPI): worker[%s]: got job[id %i] from the master: [%s]" %(myrank_str, jobid, package))
        elif isinstance(package,str):
            if package == "terminate_all_workers":
               break # terminate worker loop
        #
        # execute job command 
        #
        ret = 0
        start_time=None
        finish_time=None
        jobstatus=None
        if command is not None:
            logger.info("VGE(MPI): worker[%s]: job [id %i] has been started." %(myrank_str,jobid))
            start_time = datetime.datetime.now()
            time_command = time.time()

            # check command size
            sizecommand = len(command)
            flag_execfromscript=False

            #
            # check command size 
            #
            if sizecommand >  max_sizecommand:
                flag_execfromscript=True

                logger.info("VGE(MPI): worker[%s]: command size [%i bytes] of job [id %i] is large. [thresh=%i]" \
                          %(myrank_str,sizecommand, jobid, max_sizecommand))
                logger.info("VGE(MPI): worker[%s]: job [id %i] will be executed from script file directly." %(myrank_str,jobid))
                logger.info("VGE(MPI): worker[%s]: .... write file -> [%s]" %(myrank_str,filename))
                #
                if cl_args.nowrite_jobscript :
                   logger.info("VGE(MPI): worker[%s]: but the file will be deleted later because '--nowrite_jobscript' option turns on " %myrank_str)
                #
                vge_script_dir = mpi_args["vge_script_dir"]
                filename_script = vge_script_dir + "/" + filename
                #
                time_write2=time.time()
                #
                writefile=open(filename_script, "w")
                writefile.write(command)
                writefile.flush()
                writefile.close()
                time_write2=time.time() - time_write2
                while True:
                    if os.path.isfile(filename_script):
                         statinfo = os.stat(filename_script)
                         if int(statinfo.st_size) > sizecommand/2:
                            break
                    else:
                         time.sleep(0.00005) # 50 us wait
                filename_stdout=vge_script_dir + "/" + filename + ".o" + str(jobid)
                filename_stderr=vge_script_dir + "/" + filename + ".e" + str(jobid)
                command = "bash %s > %s 2> %s" %(filename_script, filename_stdout, filename_stderr)

            env = os.environ.copy()
            if bulkjob_id is not None:
                env["SGE_TASK_ID"] = str(bulkjob_id+1)

            #
            # popen
            #
            time_popen=time.time()
            p = subprocess.Popen(command, shell=True, bufsize=-1, stdin=None, stdout=subprocess.PIPE, \
                                    stderr=subprocess.PIPE, env=env)
            time_popen=time.time() - time_popen
            #
            jobpid=p.pid
            logger.info("VGE(MPI): worker[%s]: pid of job [id %i] ==> [%i]." %(myrank_str, jobid, jobpid))

            #
            # wait until the job is completed.
            #
            wait_count=0
            stdout=""
            stderr=""
            time_pcomm = time.time()
            #
            if flag_monitor_worker_runjob:
                #
                time_check = time.time()
                #
                flag_running_job=True
                while flag_running_job:
                   current_interval  = time.time() - time_check
                   if current_interval > time_interval_irecv_test:
                       time_check = time.time()
                       #
                       check_recv = emergency_signal_recv.test() # req.test() gives a tuple with a mesg but req.Test() logical.
                       logger.debug("VGE(MPI): worker[%s]: check_recv   ... %s " %(myrank_str,check_recv))
                       #
                       if check_recv[0] is True: # master is calling me! kill the job!
                          #
                          mesg = check_recv[1] # get a message 
                          logger.debug("VGE(MPI): worker[%s]: got a forced stop message [%s] from master when job [%i] is running" %(myrank_str, mesg,jobid))
                          logger.info( "VGE(MPI): worker[%s]: job [%i] (pid [%i]) was aborted. " %(myrank_str,jobid,jobpid))
                          #
                          try:
                              os.kill(jobpid, signal.SIGTERM) # note :OS dependent. Should be checked on K.
                          except Exception, error:
                              logger.error("VGE(MPI): worker[%s]: os.kill error was occured... [%s] " %(myrank_str,error))
                              pass
                          #
                          flag_forcedstop=True
                          flag_running_job=False
                          break # escape from poll() check loop
                          #
                       
                   if p.poll() is not None:
                      flag_running_job = False
                      break # escape from poll() check loop
                   else:
                      time.sleep(time_interval_runjobcheck)
                      wait_count +=1
                             
                #
                # job was finished.
                #
                #(stdout, stderr) = p.communicate()
                #ret = p.returncode
                if flag_forcedstop:
                   ret = -99 # stop by emergency call from master 
                   stdout="forced stop \n"
                else:
                   (stdout, stderr) = p.communicate()
                   ret = p.returncode

                #
                #
                logger.debug("VGE(MPI): worker[%s]: result of job [%i] ... stdout:[%s] stderr:[%s] ret:[%s] " %(myrank_str, jobid, stdout,stderr, ret))
                #
                logger.info( "VGE(MPI): worker[%s]: wait count of job [id %i] ==> [%i]." %(myrank_str, jobid, wait_count))
                #
                #
            else: 
                #
                # get standard and error outputs of the job
                #
                (stdout, stderr) = p.communicate()
                #
                # get a return code
                #
                ret = p.returncode
                #
                logger.debug("VGE(MPI): worker[%s]: result of job [%i] ... stdout:[%s] stderr:[%s] ret:[%s] " %(myrank_str, jobid, stdout,stderr, ret))
            #
            time_pcomm = time.time() - time_pcomm
            #

            if stdout is None:
               stdout = ""
            if stderr is None:
               stderr = ""

            # the overhead for os module was the same as that for subprocess...
            #os.system("sh ./%s" %filename)

            time_command = time.time() - time_command
            finish_time = datetime.datetime.now()
            logger.info("VGE(MPI): worker[%s]: job [id %i] is completed with code [%i]." %(myrank_str, jobid, ret))
            #
            jobstatus={}
            jobstatus["jobid"] = jobid
            jobstatus["start_time"] = start_time
            jobstatus["finish_time"] = finish_time
            jobstatus["elapsed_time"] = (finish_time - start_time).total_seconds()

            #
            # return code (shell)
            #    -15: aborted
            #    000: normally finished
            #    002: No such file or directory
            #    127: command not found
            #    139: segmentation fault (in shirokane)
            jobstatus["return_code"] = ret 
            #
            #
            if ret < 0:
               jobstatus["status"] = "aborted"
            else:
               jobstatus["status"] = "done"

            #
            # write script, standard output, and standard error
            #
            time_write=time.time()
            if cl_args.nowrite_jobscript or flag_execfromscript:
                pass # skip to write files...
                #
                # delte files...
                # 
                if cl_args.nowrite_jobscript and flag_execfromscript:
                   os.remove(filename_script)
                   os.remove(filename_stdout)
                   os.remove(filename_stderr)

            else:
                logger.info("VGE(MPI): worker[%s]: write files -> [%s]" %(myrank_str,filename))
                vge_script_dir = mpi_args["vge_script_dir"]
                #
                filename_script = vge_script_dir + "/" + filename
                try: 
                    writefile=open(filename_script, "w")
                    writefile.write(command)
                    writefile.close()
                except Exception, error:
                    logger.error("VGE(MPI): worker[%s]: write error [script] [%s] [%s] [%s]" %(myrank_str,filename, error,command))
                    pass
              
                #
                filename_stdout=vge_script_dir + "/" + filename + ".o" + str(jobid)
                try: 
                    writefile.close()
                    writefile=open(filename_stdout, "w")
                    writefile.write(stdout)
                except Exception, error:
                    logger.error("VGE(MPI): worker[%s]: write error [stdout] [%s] [%s] [%s]" %(myrank_str,filename, error,stdout))
                    pass
                #
                filename_stderr=vge_script_dir + "/" + filename + ".e" + str(jobid)
                try: 
                    writefile=open(filename_stderr, "w")
                    writefile.write(stderr)
                    writefile.close()
                except Exception, error:
                    logger.error("VGE(MPI): worker[%s]: write error [stderr] [%s] [%s] [%s]" %(myrank_str,filename, error,stdout))
                    pass
            time_write=time.time() - time_write

        else: 
            jobstatus = None

        end_job=time.time()
        time_job = end_job - start_job

        #
        #
        if jobid is not None:
           if flag_execfromscript:
               logger.info("VGE(MPI): worker[%s]: job [id %i]: elapsed times -> mesg-send[%0.3e]/recv[%0.3e] command[%0.3e] write[%0.3e]: total[%0.3e] sec" \
                   %(myrank_str, jobid, mpi_send_time, mpi_recv_time, time_command, time_write2, time_job))
           elif cl_args.nowrite_jobscript:
               logger.info("VGE(MPI): worker[%s]: job [id %i]: elapsed times -> mesg-send[%0.3e]/recv[%0.3e] command[%0.3e]/popen[%0.3e]/pcomm[%0.3e] nowrite[%0.3e]: total[%0.3e] sec" \
                   %(myrank_str, jobid, mpi_send_time, mpi_recv_time, time_command, time_popen, time_pcomm, time_write, time_job))
           else:
               logger.info("VGE(MPI): worker[%s]: job [id %i]: elapsed times -> mesg-send[%0.3e]/recv[%0.3e] command[%0.3e]/popen[%0.3e]/pcomm[%0.3e] write[%0.3e]: total[%0.3e] sec" \
                   %(myrank_str, jobid, mpi_send_time, mpi_recv_time, time_command, time_popen, time_pcomm, time_write, time_job))


    #
    # exit program
    #
    logger.info("VGE(MPI): worker[%s] finished normally." %myrank_str)

    return

#



