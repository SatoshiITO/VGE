#
#
#
def write_vge_result2(joblist1, joblist2, filename):
    #
    # joblist1 : dict
    # joblist2 : dict
    #
    lines=""
    lines+="worker_rank,job_count,work_time\n"
    
    #
    # make list to be printed
    #
    for rank, value in joblist1.items():
        lines+="%i,%i,%12.5e\n" %(rank,value,joblist2[rank])
        #print "%i,%i,%12.5e" %(rank,value,joblist2[rank])

    #
    # write file
    #
    try: 
       with open(filename,'w') as writefile:
           writefile.write(lines)

    except Exception,error:
       print "error was occured. check [%s]." %error
       pass

    return 



