#/*
# *    Virtual Grid Engine
# *
# *   (c) Copyright 2017-2019 Satoshi ITO, Masaaki Yadome, and Satoru MIYANO
# */
import random,time
import csv
import datetime

def write_vge_result1(joblist, filename):
    #
    # joblist : dict of dict
    # joblist[0] : this zero should be jobid.
    #
    #print joblist
    #print filename

    header=["jobid"]
    if 0 in joblist:
        header_dict = joblist[0].keys() # header
        header.extend(header_dict)

        try: 
           with open(filename,'wb') as f:
              #writer = csv.writer(open(filename,'wb'))
              writer = csv.writer(f)
              writer.writerow(header) 
              for jobid, job in joblist.iteritems():
                  list = [jobid]
                  for ikey, ivalue in job.iteritems():
                     list.append(ivalue)
                  writer.writerow(list) 
              f.flush()
        except Exception,error:
           print "error was occured. check [%s]." %error
           pass

    return 
#

if __name__ == '__main__':
    joblist = {0: {'status': 'done', 'sendvgetime': datetime.datetime(2016, 6, 6, 15, 38, 40, 694029), 'bulkjob_id': 0, 'finish_time': datetime.datetime(2016, 6, 6, 15, 38, 41, 550987), 'start_time': datetime.datetime(2016, 6, 6, 15, 38, 40, 697330), 'worker': 3, 'return_code': 127, 'filename': 'test_201606061538406.sh.0', 'elapsed_time': 0.853657, 'unique_jobid': 0, 'command_id': 0, 'max_task': 4, 'sendtoworker': True}, 1: {'status': 'done', 'sendvgetime': datetime.datetime(2016, 6, 6, 15, 38, 40, 694029), 'bulkjob_id': 1, 'finish_time': datetime.datetime(2016, 6, 6, 15, 38, 41, 555667), 'start_time': datetime.datetime(2016, 6, 6, 15, 38, 40, 698469), 'worker': 1, 'return_code': 127, 'filename': 'test_201606061538406.sh.1', 'elapsed_time': 0.857198, 'unique_jobid': 0, 'command_id': 0, 'max_task': 4, 'sendtoworker': True}, 2: {'status': 'done', 'sendvgetime': datetime.datetime(2016, 6, 6, 15, 38, 40, 694029), 'bulkjob_id': 2, 'finish_time': datetime.datetime(2016, 6, 6, 15, 38, 41, 553280), 'start_time': datetime.datetime(2016, 6, 6, 15, 38, 40, 698941), 'worker': 2, 'return_code': 127, 'filename': 'test_201606061538406.sh.2', 'elapsed_time': 0.854339, 'unique_jobid': 0, 'command_id': 0, 'max_task': 4, 'sendtoworker': True}, 3: {'status': 'done', 'sendvgetime': datetime.datetime(2016, 6, 6, 15, 38, 40, 694029), 'bulkjob_id': 3, 'finish_time': datetime.datetime(2016, 6, 6, 15, 38, 42, 408912), 'start_time': datetime.datetime(2016, 6, 6, 15, 38, 41, 564573), 'worker': 3, 'return_code': 127, 'filename': 'test_201606061538406.sh.3', 'elapsed_time': 0.844339, 'unique_jobid': 0, 'command_id': 0, 'max_task': 4, 'sendtoworker': True}, 4: {'status': 'wait', 'sendvgetime': datetime.datetime(2016, 6, 6, 15, 38, 54, 350521), 'bulkjob_id': 0, 'finish_time': None, 'start_time': None, 'worker': 1, 'return_code': None, 'filename': 'test_201606061538406.sh.0', 'elapsed_time': None, 'unique_jobid': 1, 'command_id': 1, 'max_task': 2, 'sendtoworker': True}, 5: {'status': 'wait', 'sendvgetime': datetime.datetime(2016, 6, 6, 15, 38, 54, 350521), 'bulkjob_id': 1, 'finish_time': None, 'start_time': None, 'worker': 2, 'return_code': None, 'filename': 'test_201606061538406.sh.1', 'elapsed_time': None, 'unique_jobid': 1, 'command_id': 1, 'max_task': 2, 'sendtoworker': True}, 6: {'status': 'ready', 'sendvgetime': datetime.datetime(2016, 6, 6, 15, 39, 0, 609090), 'bulkjob_id': 0, 'finish_time': None, 'start_time': None, 'worker': None, 'return_code': None, 'filename': 'test_201606061538406.sh.0', 'elapsed_time': None, 'unique_jobid': 2, 'command_id': 2, 'max_task': 1, 'sendtoworker': False}}


    filename ="testoutput.csv"
    #print joblist
    make_csv_joblist(joblist, filename)




