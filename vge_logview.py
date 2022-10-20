#!/usr/bin/env python3

import re
import sys
import datetime
import matplotlib.pyplot as plt


# re_job_name = re.compile('(.*)_\d{8}_\d{4}_\d{6}\.sh\.\d+')
re_job_name = re.compile('(.*)\.sh\.\d+')


def get_job_name(s):
    return re_job_name.match(s).group(1)


def color_mapper(id):
   #colormap = [ "r", "g", "b", "y", "m", "k" ]
    colormap = ["r", "g", "b", "y", "m"]
    c = id % len(colormap)
    return colormap[c]


def get_timestamp(s):
    #print('In function: s=',s)
    dt = datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S.%f")
   #return dt.timestamp()
    return dt.timestamp() / 60.0 / 60.0   # sec -> hour


if __name__ == '__main__':

    if not len(sys.argv) in [3, 5, 7]:
        print('usage: %s joblist.csv [output_prefix] [X-tics_min X-tics_max] [Y-tics_min Y-tics_max]' % sys.argv[0])
        sys.exit(1)

#     input_csv = 'test1.csv'
#     input_csv = 'test2.csv'
    input_csv = sys.argv[1]
    output_pdf = None

    if len(sys.argv) == 3:
        output_pdf = sys.argv[2]
    elif len(sys.argv) == 5:
        output_pdf = sys.argv[2]
        x_min = float(sys.argv[3])
        x_max = float(sys.argv[4])
    elif len(sys.argv) == 7:
        output_pdf = sys.argv[2]
        x_min = float(sys.argv[3])
        x_max = float(sys.argv[4])
        y_min = int(sys.argv[5])
        y_max = int(sys.argv[6])


    f = open(input_csv, 'r', newline=None)
    f.readline()  # skip header

    job_list = []
    start = datetime.datetime.now().timestamp()

    for line in f:
        (jobid, status, sendvgetime, bulkjob_id, execjobid,
         finish_time, start_time, worker, return_code,
         filename, genomon_parent_pid, elapsed_time, max_task,
         genomon_pid, command_id, unique_jobid, sendtoworker) = line.rstrip('\n').split(',')

        if return_code == '':
          continue

        #print('sendvgettime =',sendvgetime,'Lastcheck')
        #print('start_time =',start_time)
        start = min(start, get_timestamp(sendvgetime))
        s = get_timestamp(start_time)
        f = get_timestamp(finish_time)
        #print(jobid, bulkjob_id, genomon_pid, unique_jobid)
        #print(unique_jobid, filename)
        #print(jobid, unique_jobid, bulkjob_id, filename, max_task)

        job_list.append((int(worker), s, f, int(unique_jobid)))

        if bulkjob_id == '0':
            unique_job = get_job_name(filename)
            if max_task != '0':
                unique_job += ' (x' + max_task + ')'
            print(unique_jobid, unique_job)

    #print(start)
    #print(job_list)

    for node, s, f, id in job_list:
       #plt.hlines(node, s-start, f-start, colors=color_mapper(id), lw=5)
        plt.hlines(node, s - start, f - start, colors=color_mapper(id), lw=1)
       #plt.text(s-start, node+0.25, str(id), fontsize=9)

   #plt.xlabel('Time (sec)')
    plt.xlabel('Time (hour)')
   #plt.ylabel('Nodes')
    if output_pdf:
        #import matplotlib
        #from matplotlib.backends.backend_pdf import PdfPages
        #matplotlib.rcParams['pdf.fonttype'] = 42
        #matplotlib.rcParams['savefig.dpi'] = 300
        #pdf = PdfPages(output_pdf)
        #pdf.savefig()
        #pdf.close()
        plt.xlabel('Time (hours)')
        plt.ylabel('Worker number')

        if len(sys.argv)==7:
          plt.xlim(x_min,x_max)
          plt.ylim(y_min,y_max)
        elif len(sys.argv)==5:
          plt.xlim(x_min,x_max)

        fname='{0}.eps'.format(output_pdf)
        print(fname)
        plt.savefig(fname, format='eps', dpi=300)
        fname='{0}.png'.format(output_pdf)
        plt.savefig(fname, format='png', dpi=300)
        #fname='{0}.pdf'.format(output_pdf)
        #plt.savefig(fname', format='pdf', dpi=300)
    else:
        plt.show()
