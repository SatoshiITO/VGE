import random,time
import datetime

def job():
    """ the jobstatus keylist that is send to VGE"""

    #
    # command
    # VGE_BULK_ID in "command" will be replace with bulk_id (0,1,2, ... ,maxtask-1).
    #
    nsleep = 10.0
    command =""
    command +="#!/bin/bash\n"
    #icount = int(random.random()*50000 + 1000)
    #temp =  "# icount = %10i \n" %icount
    #command += temp
    #for ii in range(icount):
    #   temp =  "# %10i testtesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttesttest \n" %ii
    #   command += temp
    command += '''sleep %10.4f\n''' %nsleep

    #
    # job status
    #
    # status = ready ... this job is ready to be send to a worker in VGE. 
    # status = wait ...  wait for the job to finish
    # status = done ...  the job was done with a return code.
    job={}
    job["status"]="ready"
    job["command"]=command

    #
    # a basefilename for the job
    #
    job["basefilename"]="test_"+(str(datetime.datetime.now().strftime("%Y%m%d_%H%M_%S%f")))[:-5] # ex. 20160603182033811801 -> 201606031820338

    #
    # an unique jobid used in both GenomonPipeline and VGE
    #
    job["unique_jobid"]=None

    # start jobid used in VGE 
    # if this job is bluk type then,finish jobid should be jobid + max_task)
    job["vge_jobid"]=None

    #
    # submit time to VGE
    #
    job["sendvgetime"]=None

    #
    # return code.  for bulk job, if the jobs have code > 0 then, this code will have the largest code
    #
    job["return_code"]=None

    #
    # max task (number of bluk jobs)
    #
    max_task=30
    #
    job["max_task"]=max_task 

    #
    # process id for GenomonPipeline
    #
    job["genomon_pid"]=-1


    return job

def make_genomon_jobstatuslist(max_of_jobs):

    joblist={}
    if max_of_jobs==0:
       joblist = job()
    else:
      for i in range(max_of_jobs):
          joblist[i] = {}
          joblist[i] = job()

    return joblist

#
if __name__ == '__main__':
    print ""
    print make_genomon_jobstatuslist(0)
    print ""
    #print make_genomon_jobstatuslist(200000)
    
    #import sys, cPickle
    #print sys.getsizeof(cPickle.dumps((make_genomon_jobstatuslist(200000))))


