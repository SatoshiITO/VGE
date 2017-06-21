from mpi4py import MPI
import subprocess
import os
import commands
import math, datetime, time
from logging import getLogger,StreamHandler,basicConfig,DEBUG,INFO,WARNING,ERROR,CRITICAL
#
from VGE.get_vge_conf import get_vge_conf

def mpi_jobcontroll_worker(cl_args, mpi_args, comm):
    #
    logger=getLogger(__name__)
    logger.setLevel(INFO)

    # 
    # set verbose level
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
    max_sizecommand = 200000
    max_sizecommand = int(get_vge_conf("vge","mpi_command_size",max_sizecommand))
    logger.info("VGE(MPI): worker[%s]: max. command size = %i" %(myrank_str, max_sizecommand))

    # job status
    jobstatus = None
    logger.info("VGE(MPI): MPI worker[%s] is running." %myrank_str)
    command = ""
    ret = 0 # return code from subprocess


    #//////////////////////////
    # job loop for worker nodes
    #//////////////////////////
    while True:
        #
        # request a job and send the previous job status to the master
        #
        start_job=time.time()

        start_time=time.time()
        comm.send(jobstatus, dest=0, tag=myrank) 
        end_time=time.time()
        mpi_send_time=end_time - start_time
        if jobstatus is not None:
            logger.info("VGE(MPI): worker[%s]: send job status to the master: [%s]" %(myrank_str, jobstatus))

        #
        # get an order from the master 
        #
        start_time=time.time()
        package = comm.recv(source=0, tag=MPI.ANY_TAG, status=status)
        end_time=time.time()
        mpi_recv_time=end_time - start_time

        command=None
        filename=None
        jobid=None
        if isinstance(package,dict):
            command = package["command"]
            filename = package["filename"]
            jobid  = package["jobid"]
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
            if sizecommand >  max_sizecommand:
                flag_execfromscript=True

                logger.info("VGE(MPI): worker[%s]: command size [%i bytes] of job [id %i] is large. [thresh=%i]" \
                          %(myrank_str,sizecommand, jobid, max_sizecommand))
                logger.info("VGE(MPI): worker[%s]: job [id %i] will be executed from script file directly." %(myrank_str,jobid))
                logger.info("VGE(MPI): worker[%s]: .... write file -> [%s]" %(myrank_str,filename))

                vge_script_dir = mpi_args["vge_script_dir"]
                filename_script = vge_script_dir + "/" + filename
                time_write2=time.time()
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
                command2 = "bash %s > %s 2> %s" %(filename_script, filename_stdout, filename_stderr)
                ret = os.system(command2)

            else:
                #
                # popen
                #
                time_popen=time.time()
                p = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, \
                                    stderr=subprocess.PIPE)
                time_popen=time.time() - time_popen

                #
                # wait until the job is completed.
                #
                stdout=""
                stderr=""
                time_pcomm = time.time()
                (stdout, stderr) = p.communicate()
                time_pcomm = time.time() - time_pcomm
                if stdout is None:
                   stdout = ""
                if stderr is None:
                   stderr = ""
           
                #
                # get a return code
                #
                ret = p.returncode

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
            jobstatus["status"] = "done"
            #
            # return code (shell)
            #    000: normally finished
            #    002: No such file or directory
            #    127: command not found
            jobstatus["return_code"] = ret

            #
            # write script, standard output, and standard error
            #
            time_write=time.time()
            if cl_args.nowrite_jobscript or flag_execfromscript:
                pass # skip to write files...
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
               logger.info("VGE(MPI): worker[%s]: job [id %i]: elapsed times -> mesg-send[%8.3e]/recv[%8.3e] command[%8.3e] write[%8.3e]: total[%8.3e] sec" \
                   %(myrank_str, jobid, mpi_send_time, mpi_recv_time, time_command, time_write2, time_job))
           elif cl_args.nowrite_jobscript:
               logger.info("VGE(MPI): worker[%s]: job [id %i]: elapsed times -> mesg-send[%8.3e]/recv[%8.3e] command[%8.3e]/popen[%8.3e]/pcomm[%8.3e] nowrite[%8.3e]: total[%8.3e] sec" \
                   %(myrank_str, jobid, mpi_send_time, mpi_recv_time, time_command, time_popen, time_pcomm, time_write, time_job))
           else:
               logger.info("VGE(MPI): worker[%s]: job [id %i]: elapsed times -> mesg-send[%8.3e]/recv[%8.3e] command[%8.3e]/popen[%8.3e]/pcomm[%8.3e] write[%8.3e]: total[%8.3e] sec" \
                   %(myrank_str, jobid, mpi_send_time, mpi_recv_time, time_command, time_popen, time_pcomm, time_write, time_job))


    #
    # exit program
    #
    logger.info("VGE(MPI): worker[%s] finished normally." %myrank_str)

    return

#



