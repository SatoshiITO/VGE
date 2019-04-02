#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
import random,time

def make_vge_jobstatuslist():
    """ added job status list in VGE   """

    #
    job={}

    # ordered id for executed job 
    job["execjobid"]=None

    # flag for send to a work 
    # flag will be changed to True if VGE already send a job to a woker
    job["sendtoworker"]=False 

    # worker rank number for this job
    job["worker"]=None

    # filename
    job["filename"]=None

    # id for bulk job (=0, 1, 2  .... , max_task-1)
    job["bulkjob_id"]=None

    # command_id
    # exact command(s) is deleted in total_joblist and stored in command_list[unique_jobid] in VGE
    job["command_id"]=None

    # start time
    job["start_time"]=None

    # finish time
    job["finish_time"]=None

    # elapsed time in second (calculated the difference between finish and start times)
    job["elapsed_time"]=None

    return job

#
if __name__ == '__main__':
    print ""
    print make_vge_jobstatuslist()
    print ""



